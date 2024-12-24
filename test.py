from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import StructType, StructField, IntegerType, StringType
import os
import secret_functions
import pandas

# Establish a Snowpark session
session = Session.builder.configs({
    "account": os.environ.get("SM_SF_ACCOUNT") + "." + os.environ.get("SM_SF_REGION"),
    "user": os.environ.get("SM_SF_USER"),
    "password": os.environ.get("SM_SF_PWD"),
    "role": os.environ.get("SM_SF_ROLE"),
    "warehouse": os.environ.get("SM_SF_WAREHOUSE"),
    "database": os.environ.get("SM_SF_CMS_SRE_DB"),
    "schema": os.environ.get("SM_SF_CMS_SCHEMA")
}).create()

def main (session, PROD, NONPROD, LAB):


    # Create an empty DataFrame with specified schema
    schema = StructType([
        StructField("acct_type", StringType())
    ])
    df = session.create_dataframe([], schema)

    ENVS = []
	
    if PROD == 1:
        ENVS.append('Prod')

    if NONPROD == 1:
        ENVS.append('Non Prod')

    if LAB == 1:
        ENVS.append('Lab')

    for item in ENVS:
        new_row = session.create_dataframe([item], schema)
        df = df.union(new_row)
    pass
    # Identify if there is already a record matching criteria
    DEST_TABLE = session.table('CA_SITE_RELIABILITY_ENGINEERING_CMS.CMS_CORE_DATA.VW_AWS_ACCOUNTS').filter( \
    	(col("\"portfolio_name\"").like('Consumer%Marketing Solutions')) &  \
    	(col("\"env\"").isin(df)) \
    ).select("\"aws_account_id\"")

    response = (DEST_TABLE.to_pandas()).to_dict('records')
    return response

main(session,1,1,0)