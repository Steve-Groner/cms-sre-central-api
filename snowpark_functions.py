from snowflake.snowpark import Session
import os
import json

class SnowflakeConnector:
    def __init__(self):
        self.session = self.connect_to_snowflake()

    def connect_to_snowflake(self):
        """
        Creates Connection to Snowflake via Snowpark
        """
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
        return session
    
def get_stored_procedure_data(session: Session, stored_procedure_name: str, prod: int, nonprod: int, lab: int):

    params = {
        "prod": int(prod),
        "nonprod": int(nonprod),
        "lab": int(lab)
    }
    
    # Execute the stored procedure with parameters
    result = session.call(stored_procedure_name, *params.values())
    
    return result.to_pandas().to_dict('records')

# Initialize the connection when the module is imported
snowflake_connector = SnowflakeConnector()

# Export the connection for use elsewhere
__all__ = ['snowflake_connector']


