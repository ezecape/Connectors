from google.cloud import bigquery
from google.oauth2 import service_account
import json
from aws_services.ssm import ParameterStore
from api_connectors.utils import df_to_csv
from api_connectors.utils import process_other_data

class GoogleBigQuery:
    """
    This class integrates the Google BigQuery API to access the googlebigquery tables
    """
    def __init__(self, access_params):
        assert "project_id" in access_params.keys(), "access_params must have a project_id"
        assert "json_info" in access_params.keys(), "access_params must have a json_info"
        
        info = access_params["json_info"] 

        self.credentials = service_account.Credentials.from_service_account_info(info)
        
        self.project_id = access_params["project_id"]
        
        self.client = bigquery.Client(credentials= self.credentials,project=self.project_id)

    def request(self, config_params,request_params):
        assert "process_other_data" in config_params.keys(), "config_params must have a process_other_data key"
        assert "query" in request_params.keys(), "request_params must have a query key"
      
        query = request_params["query"]

        query_job = self.client.query(query)  # API request
        
        df_query_job = query_job.to_dataframe()
        
        if config_params["process_other_data"]:
            df_query_job=process_other_data(df_query_job)

        response = df_to_csv(df_query_job)

        return response

