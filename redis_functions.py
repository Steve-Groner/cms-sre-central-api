import redis
from time import time
from typing import List, Dict, Any
import json
import uuid
from snowpark_functions import snowflake_connector

global session
global r

#REDIS_HOST = 'ig-cache-cluster.9kqmvg.ng.0001.use1.cache.amazonaws.com'
REDIS_HOST = '54.145.94.191'
REDIS_PORT = 6379
REDIS_DB = 0

rpool = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB,decode_responses=True)
r = redis.StrictRedis(connection_pool=rpool,encoding='utf-8')

def timer_func(func): 
    """
    A decorator that measures the execution time of a function and prints it.

    Args:
        func (callable): The function to be wrapped and timed.

    Returns:
        callable: The wrapped function that includes timing functionality.

    Example:
        @timer_func
        def my_function():
            # Function implementation
            pass
    """
    def wrap_func(*args, **kwargs): 
        t1 = time() 
        result = func(*args, **kwargs) 
        t2 = time() 
        print(f'Function {func.__name__!r} executed in {(t2-t1):.4f}s') 
        return result 
    return wrap_func 

@timer_func
def set_key_value(r, key, value):
    """Set a key-value pair in Redis."""
    r.set(key, value)

@timer_func
def get_value_by_key(r, key):
    """Get the value of a key from Redis."""
    return r.get(key)

@timer_func
def delete_key(r, key):
    """Delete a key from Redis."""
    r.delete(key)

@timer_func
def key_exists(r, key):
    """Check if a key exists in Redis."""
    return r.exists(key)

def increment_key(r, key, amount=1):
    """Increment the value of a key in Redis."""
    r.incr(key, amount)

@timer_func
def search_by_key(parent_key: str, namespace: str = '', value: Any = None) -> List[Dict[str, Any]]:
    """
    Search for a specific key in a Redis database and return its associated value.

    Args:
        parent_key (str): The parent key to search under.
        namespace (str, optional): The namespace to prepend to the key. Defaults to an empty string.
        value (Any, optional): The specific value to search for. Must be provided.

    Returns:
        List[Dict[str, Any]]: A list containing the result as a dictionary if found, otherwise an empty list.

    Raises:
        ValueError: If the value parameter is not provided.
    """

    if value is not None:
        result = r.hgetall(f"{namespace}:{parent_key}:{value}")
        return [result] if result else []
    else:
        raise ValueError('Value parameter cannot be empty.')

@timer_func
def delete_keys(parent_key: str, namespace: str = '') -> None:
    """
    Deletes all keys in Redis that match the given parent key pattern within the specified namespace.
    Args:
        parent_key (str): The parent key pattern to match for deletion.
        namespace (str, optional): The namespace to prepend to the parent key pattern. Defaults to an empty string.
    Returns:
        None
    """
    
    full_parent_key = f'{namespace}:{parent_key}*'
    keys_to_delete = list(r.scan_iter(full_parent_key))
    
    if keys_to_delete:
        r.delete(*keys_to_delete)
    
    print(f"Deleted keys matching pattern: {full_parent_key}.")

@timer_func
def get_all_keys(parent_key: str, namespace: str = '') -> List[Dict[str, Any]]:
    """
    Retrieve all hash keys and their values from a Redis database that match a given pattern.
    Args:
        parent_key (str): The parent key to match against in the Redis database.
        namespace (str, optional): An optional namespace to prepend to the parent key. Defaults to an empty string.
    Returns:
        List[Dict[str, Any]]: A list of dictionaries containing the hash keys and their corresponding values.
    Raises:
        Exception: If there is an error retrieving the keys from the Redis database.
    """
    pattern = f'{namespace}:{parent_key}:*'

    lua_script = """
    local cursor = "0"
    local pattern = ARGV[1]
    local hash_keys = {}

    repeat
        local result = redis.call("SCAN", cursor, "MATCH", pattern)
        cursor = result[1]
        local keys = result[2]

        for _, key in ipairs(keys) do
            if redis.call("TYPE", key).ok == "hash" then
                table.insert(hash_keys, key)
            end
        end
    until cursor == "0"

    return hash_keys
    """
    
    try:
        hash_keys = r.eval(lua_script, 0, pattern)
        
        pipeline = r.pipeline()
        for key in hash_keys:
            pipeline.hgetall(key)
        
        hash_values = pipeline.execute()
        
        return hash_values
    except Exception as e:
        print(f"Error retrieving keys: {e}")
        return []
    
@timer_func
def search_parent_key_by_index(filter):
    """
    Searches for parent keys in Redis based on the provided filter criteria and retrieves associated hashes.

    Args:
        filter (dict): A dictionary containing the following keys:
            - operation (str): The operation to perform on the results ('intersect' or 'union').
            - namespace (str): The namespace to use for the keys.
            - parent_key (str): The parent key to use for the keys.
            - criteria (list): A list of dictionaries, each containing:
                - index (str): The index to search for.
                - criteria (str): The criteria to match.
            - sortby (str, optional): The key to sort the final results by.
            - sortorder (str, optional): The order to sort the final results ('asc' or 'desc').
            - top_n (int or str, optional): The number of top results to return. Use 'ALL' to return all results.

    Returns:
        list: A list of dictionaries containing the matching members and their associated hashes.
    """

    operation = filter['operation']
    namespace = filter['namespace']
    parent_key = filter['parent_key']

    lua_search_script = """
        local pattern = ARGV[1]
        local cursor = "0"
        local matching_members = {}
        repeat
            local result = redis.call("SCAN", cursor, "MATCH", pattern)
            cursor = result[1]
            local keys = result[2]
            for i, key in ipairs(keys) do
                local members = redis.call("SMEMBERS", key)
                for j, member in ipairs(members) do
                    table.insert(matching_members, member)
                end
            end
        until cursor == "0"
        return cjson.encode(matching_members)
    """

    lua_hashes_script = """
    local members = cjson.decode(ARGV[1])
    local namespace = ARGV[2]
    local parent_key = ARGV[3]
    local associated_hashes = {}
    for i, member in ipairs(members) do
        local hash_key = namespace .. ":" .. parent_key .. ":" .. member  -- Assuming the hash keys are prefixed with namespace and parent_key
        local hashes = redis.call("HGETALL", hash_key)
        if next(hashes) == nil then
            table.insert(associated_hashes, {member=member, hashes="No hashes found"})
        else
            local hash_dict = {member=member}
            for j=1,#hashes,2 do
                hash_dict[hashes[j]] = hashes[j+1]
            end
            table.insert(associated_hashes, hash_dict)
        end
    end
    return cjson.encode(associated_hashes)
    """

    # Register the Lua script with Redis
    search_sets_script = r.register_script(lua_search_script)
    get_hashes_script = r.register_script(lua_hashes_script)

    master_list = []

    for criterion in filter['criteria']:
        index = criterion['index']
        criteria = criterion['criteria']

        # Create the pattern to search for
        pattern = f'{namespace}:{parent_key}:{index}:{criteria}'

        matching_members = []

        # Get Members
        try:
            matching_members_json = search_sets_script(args=[pattern])
            matching_members = json.loads(matching_members_json)
        except Exception as e:
            for arg in e.args:
                if 'Script killed by user with SCRIPT KILL' in arg:
                    print('To many results, please narrow down your search (script time limit reached on redis server)')
                else:
                    print(f'Error: {arg}')
            return []

        # Add to master list
        master_list.append(matching_members)

    if operation == 'intersect':
        matching_members = list(set(master_list[0]).intersection(*map(set, master_list[1:])))
    elif operation == 'union':
        matching_members = list(set(master_list[0]).union(*map(set, master_list[1:])))

    try:

        final_hashes = []

        # Define the chunk size
        chunk_size = 100000

        # Process the list in chunks
        for i in range(0, len(matching_members), chunk_size):
            chunk = matching_members[i:i + chunk_size]
            # Process the chunk
            # print(f"Processing chunk from index {i} to {i + chunk_size - 1}")
            # Add your processing logic here
            members_json = json.dumps(chunk)
            associated_hashes_json = get_hashes_script(args=[members_json, namespace, parent_key])
            associated_hashes = json.loads(associated_hashes_json)
            final_hashes.extend(associated_hashes)

    except Exception as e:
        for arg in e.args:
            if 'Script killed by user with SCRIPT KILL' in arg:
                print('To many results, please narrow down your search (script execution time limit reached on redis server)')
            else:
                print(f'Error: {arg}')
        return []

    if 'sortby' in filter:
        sortby = filter['sortby']
        sortorder = filter['sortorder']
        final_hashes = sorted(final_hashes, key=lambda x: x[sortby], reverse=True if sortorder == 'desc' else False)

    if 'top_n' in filter:
        topn = filter['top_n']
        if topn != 'ALL':
            final_hashes = final_hashes[:topn]

    return final_hashes

def hydrate_data(table_name, namespace='', primary_key_name='GUID', filter=[], filter_operator='ALL',secondary_index_list=[]):

    session = snowflake_connector.session

    redis_parent_key = f"{namespace}:{table_name.split('.')[-1]}"

    # Example of Key Deletion
    delete_keys(f'{redis_parent_key}')

    # Create a DataFrame from a Snowflake table
    df_base = session.table(f"{table_name}")

    # Build the filter from filter list
    filter_list = []

    f = None
    
    if filter != None:
        for dictionary in filter:
            for k, v in dictionary.items():
                entry = f"(\"{k}\" = '{v}')"
                filter_list.append(entry)
    
        # Join all the filter entries
        f = f" {filter_operator} ".join(filter_list)

    # Perform any transformations if needed
    if f != None and f != '':
        df_filtered = df_base.filter(f)
    else:
        df_filtered = df_base
    
    pandas_df = df_filtered.to_pandas()

    json_list = []

    for index, row in pandas_df.iterrows():
        json_string = row.to_json()
        json_list.append(json.loads(json_string))

    json_arr = json_list

    max_pipe_depth = 10000
    current_pipe_depth = 0

    pipe = r.pipeline()

    # Store data in Redis
    for row in json_arr:
        
        if primary_key_name == 'GUID':
            primary_key_value = str(uuid.uuid4())
        else:
            primary_key_value = row[f'{primary_key_name}']  # Assuming the first column is the unique ID
        modified_row = {k: (str(v if v is not None else '')) for i, (k, v) in enumerate(sorted(row.items()))}
        
        pipe.hset(f"{redis_parent_key}:{primary_key_value}", mapping=modified_row)
        current_pipe_depth += 1

        print(f"Hydrating Parent: {redis_parent_key}:{primary_key_value} ...")

        for index in secondary_index_list:

            index_list = index.split(':')
            t_list = []
            if len(index_list) > 1:
                for index_entry in index_list[1:]:
                    t_list.append(f"{str(row[f'{index_entry}'])}")
            else:
                t_list.append(f"{str(row[f'{index_list[0]}'])}")
            
            index_value = "|".join(t_list)
            
            pipe.sadd(f"{redis_parent_key}:{str(index_list[0])}:{index_value}", primary_key_value)
            current_pipe_depth += 1

            print(f"  --  Creating Secondary Index: {redis_parent_key}:{str(index_list[0])}:{index_value} with {primary_key_value} ...")

        if current_pipe_depth > max_pipe_depth:
            pipe.execute()
            current_pipe_depth = 0
            print(f"Writing 10K Records ...")

    pipe.execute()
    print(f"Writing Final Records ...")

if __name__ == '__main__':
    
    #------------------------------------------------------------
    # Example of Usage - search_parent_key_by_index

    """ filters = {
        'operation': 'intersect', # or 'union'
        'namespace': 'instance_guardian',
        'parent_key': 'VW_ALL_AMI_DATA',
        'sortby': 'CREATION_DATE',
        'sortorder': 'asc',
        'top_n': 100,
        'criteria': [
            {
                'index': 'NAME',
                'criteria': 'al2023*'
            },
            {
                'index': 'REGION',
                'criteria': 'us-e*'
            }
        ]
    } """

    # Search sets that match the pattern and get all members
    """ matching_members = search_parent_key_by_index(filters) """

    #------------------------------------------------------------
    # Example of Usage - get_all_keys

    """ rts = get_all_keys(namespace='general_namespace',parent_key='VW_RELEASE_TRAINS') """

    #------------------------------------------------------------
    # Example of Usage - search_by_key

    """ record = search_by_key(parent_key='VW_SNOW_WFM_WORKLOADS_COMPONENTS',namespace='ig_namespace',value='CI1069954') """

    # Set a key-value pair
    """ set_key_value(r, 'example_key', 'example_value') """

    # Get the value of a key
    """ value = get_value_by_key(r, 'example_key')
    print(f'Value for "example_key": {value}') """

    # Check if a key exists
    """ exists = key_exists(r, 'example_key')
    print(f'Does "example_key" exist? {exists}') """

    # Increment the value of a key
    """ increment_key(r, 'example_counter')
    counter_value = get_value_by_key(r, 'example_counter')
    print(f'Value for "example_counter": {counter_value}') """

    # Delete a key
    """ delete_key(r, 'example_key')
    exists = key_exists(r, 'example_key')
    print(f'Does "example_key" exist after deletion? {exists}') """

    # Example Hydrate with FILTER
    # Define Filter Object
    """ filter = [
        {"portfolio_name": "Consumer & Marketing Solutions"}
    ]
    hydrate_data(table_name='CA_SITE_RELIABILITY_ENGINEERING_CMS.CMS_CORE_DATA.VW_SNOW_WFM_WORKLOADS_COMPONENTS',namespace=redis_namespace, primary_key_name='id',filter=filter, secondary_index_list=['is_workload'])
    """