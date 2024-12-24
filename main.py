from fastapi import FastAPI
from models import MsgPayload
import secret_functions
from snowpark_functions import get_stored_procedure_data, snowflake_connector
from redis_functions import search_parent_key_by_index, search_by_key
import os
import re
import json
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, Response
from fastapi import Body, Cookie, File, Form, Header, Path, Query
from fastapi.middleware.gzip import GZipMiddleware


app = FastAPI()
messages_list: dict[int, MsgPayload] = {}

app.add_middleware(GZipMiddleware, minimum_size=1000, compresslevel=5)

def get_all_values(list_of_dictionaries):
    return [value for dictionary in list_of_dictionaries for value in dictionary.values()]


@app.get("/healthcheck")
def healthcheck():
    return {"status": "healthy"}

@app.get("/get_accounts")
def getAccoutData(prod: int | None = None, nonprod: int | None = None, preprod: int | None = None, lab: int | None = None, dev: int | None = None, level: int | None = None, fields: str | None = None, aws_account_ids_only: bool | None = False):
    """
    Retrieve account data based on specified criteria.
    Args:
        prod (int | None): Filter for production environment (1 for true, None for false).
        nonprod (int | None): Filter for non-production environment (1 for true, None for false).
        preprod (int | None): Filter for pre-production environment (1 for true, None for false).
        lab (int | None): Filter for lab environment (1 for true, None for false).
        dev (int | None): Filter for development environment (1 for true, None for false).
        level (int | None): Filter for account level (1 or 2, None for both).
        fields (str | None): Comma-separated string of fields to include in the result.
        aws_account_ids_only (bool | None): If True and only one field is specified, return only the values of that field (if not more than (1) field).
    Returns:
        JSONResponse: A JSON response containing the filtered account data.
    """

    criteria = []

    if fields != None:
        fields = (fields.upper()).split(',')

    if level == None:
        criteria.append({'index':'LEVEL','criteria':'1'})
        criteria.append({'index':'LEVEL','criteria':'2'})
    elif level == 1:
        criteria.append({'index':'LEVEL','criteria':'1'})
    elif level == 2:
        criteria.append({'index':'LEVEL','criteria':'2'})
    
    if dev != 1:
        if prod == 1:
            criteria.append({'index':'ENV','criteria':'Prod'})

        if nonprod == 1:
            criteria.append({'index':'ENV','criteria':'Non Prod'})

        if preprod ==1:
            criteria.append({'index':'ENV','criteria':'Pre Prod'})

        if lab == 1:
            criteria.append({'index':'ENV','criteria':'Lab'})

    else:
        criteria.append({'index':'ENV','criteria':'Prod'})
        criteria.append({'index':'ENV','criteria':'Non Prod'})
        criteria.append({'index':'ENV','criteria':'Pre Prod'})

    filters = {
        'operation': 'union', # or 'union'
        'namespace': 'cms_sre',
        'parent_key': 'VW_AWS_ACCOUNTS',
        'sortby': 'AWS_ACCOUNT_ID',
        'sortorder': 'asc',
        'top_n': 1000,
        'criteria': criteria
    }

    # Search sets that match the pattern and get all members
    matching_members = search_parent_key_by_index(filters)

    if dev == 1:
        matching_members = [acct for acct in matching_members if acct['AWS_ACCOUNT_ID'] in ['631487181643','473451415060','402718907337','841464455146','906105512948']]

    if fields != None:
        
        keys_to_extract = fields
        matching_members = [{key: dictionary[key] for key in keys_to_extract if key in dictionary} for dictionary in matching_members]

    if aws_account_ids_only and len(fields) == 1:
        matching_members = get_all_values(matching_members)

    json_compatible_item_data = jsonable_encoder(matching_members)
    return JSONResponse(content=json_compatible_item_data)

def main():
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=3001)

if __name__ == "__main__":
#def main():
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=3001)