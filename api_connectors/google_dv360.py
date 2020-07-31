from aws_services.ssm import ParameterStore
import subprocess
import os
from api_connectors.utils import S3Buckets, get_logger

class Dv360:
    """
    This class is used for executing a request to Google DV360
    """
    def __init__(self):
        self.logger = get_logger()

    def request(self, config_params, request_params):
        # Get client_secret from ParameterStore
        self.logger.info("Getting client secret...")
        self.get_parameter_(credential="secret")
        # Get .dat file from ParameterStore
        self.logger.info("Getting .dat file...")
        self.get_parameter_(credential="dat")
        # Set dir
        abspath = os.path.abspath(__file__)
        dname = os.path.dirname(abspath)
        new_path = os.path.join(dname, "google_dv360_cli.py")
        self.logger.info("Getting path for google_dv360_cli.py: %s", new_path)
        self.logger.info("Starting Google DV360 request using CLI")
        # Executes request and get output from google_dv360_cli.py
        results = subprocess.check_output("python " + new_path + """ --query_id {0} --bad_lines {1}""".format(request_params["query_id"],request_params["bad_lines"]), shell=True)
        self.logger.info("Google DV360 request using CLI finished")
        # Put .dat in ParameterStore.
        self.put_dat_parameter_()
        self.logger.info("Putting .dat file...")
        return results

    def get_parameter_(self, credential):
        ssm_client = ParameterStore(region_name=S3Buckets.REGION_NAME)
        self.logger.info("ParameterStore client instantiated: %s", ssm_client)

        if credential == "dat":
            param,version = ssm_client.get_parameter("google_dv360_store")
            with open("google_dv360_store_file.dat", "w") as f:
                f.write(param)
        elif credential == "secret":
            param,version = ssm_client.get_parameter("google_dv360_secret")  
            with open("google_dv360_client_secret.json", "w") as f:
                f.write(param)
        else:
            self.logger.info("Incorrect credential type, it must be either dat or secret")
            raise ValueError("Incorrect credential type, it must be either dat or secret")

    def put_dat_parameter_(self):
        ssm_client = ParameterStore(region_name=S3Buckets.REGION_NAME)
        self.logger.info("ParameterStore client instantiated: %s", ssm_client)
        with open("google_dv360_store_file.dat", "r") as f:
            new_param=f.read()
        ssm_client.put_parameter(name="google_dv360_store_file", value=new_param, parameter_type='SecureString')
