from aws_services.ssm import ParameterStore
import subprocess
import os
from api_connectors.utils import S3Buckets, get_logger


class YouTube:
    """
    This class is used for executing a request to YouTube Analytics API - V.2
    """
    def __init__(self):
        self.channel_id = None
        self.channel_name = None
        self.logger = get_logger()

    def request(self, config_params, request_params):
        # Get channel_id and channel_name from config_params
        self.logger.info("Getting attributes...")
        self.get_attributes_(config_params)
        # Get client_secret from ParameterStore
        self.logger.info("Getting client secret...")
        self.get_parameter_(credential="secret")
        # Get .dat file from ParameterStore
        self.logger.info("Getting .dat file...")
        self.get_parameter_(credential="dat")
        # Set dir
        abspath = os.path.abspath(__file__)
        dname = os.path.dirname(abspath)
        new_path = os.path.join(dname, "youtube_cli.py")
        self.logger.info("Getting path for youtube_cli.py: %s", new_path)
        self.logger.info("Starting YouTube request using CLI")
        # Executes request and get output from youtube_cli.py
        results = subprocess.check_output("python " + new_path + """ --channel_id {0} --fox_channel_name {1} --metrics {2} --dimensions {3} --max_results {4}""".format(self.channel_id, self.channel_name, request_params["metrics"],
                                                      request_params["dimensions"], request_params["max_results"]), shell=True)
        self.logger.info("YouTube request using CLI finished")
        # Put .dat in ParameterStore.
        self.put_dat_parameter_()
        self.logger.info("Putting .dat file...")
        return results

    def get_parameter_(self, credential):
        ssm_client = ParameterStore(region_name=S3Buckets.REGION_NAME)
        self.logger.info("ParameterStore client instantiated: %s", ssm_client)

        if credential == "dat":
            param,version = ssm_client.get_parameter("youtube_dat_"+self.channel_name)
            with open(self.channel_name + ".dat", "w") as f:
                f.write(param)
        elif credential == "secret":
            param,version = ssm_client.get_parameter("youtube_secret")  
            with open("yt_client_secret.json", "w") as f:
                f.write(param)
        else:
            self.logger.info("Incorrect credential type, it must be either dat or secret")
            raise ValueError("Incorrect credential type, it must be either dat or secret")

    def put_dat_parameter_(self):
        ssm_client = ParameterStore(region_name=S3Buckets.REGION_NAME)
        self.logger.info("ParameterStore client instantiated: %s", ssm_client)
        with open(self.channel_name + ".dat", "r") as f:
            new_param=f.read()
        ssm_client.put_parameter(name="youtube_dat_"+self.channel_name, value=new_param, parameter_type='SecureString')

    def get_attributes_(self, config_params):
        self.logger.info("config_params: %s", config_params)
        self.channel_id = config_params["channel_id"]
        self.channel_name = config_params["channel_name"]
