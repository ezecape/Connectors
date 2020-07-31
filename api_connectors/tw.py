import twitter
from api_connectors.utils import iterate_flatten, df_to_csv
import pandas as pd


class Twitter:
    """
    This class integrates the Twitter API to access the raw tweets data
    """
    def __init__(self, access_params):
        assert "consumer_key" in access_params.keys(), "access_params must have a consumer_key"
        assert "consumer_secret" in access_params.keys(), "consumer_secret must have a consumer_key"
        assert "access_token" in access_params.keys(), "access_token must have a consumer_key"
        assert "access_token_secret" in access_params.keys(), "access_token_secret must have a consumer_key"
        self.consumer_key = access_params["consumer_key"]
        self.consumer_secret = access_params["consumer_secret"]
        self.access_token = access_params["access_token"]
        self.access_token_secret = access_params["access_token_secret"]
        self.api = twitter.Api(consumer_key=self.consumer_key, consumer_secret=self.consumer_secret,
                               access_token_key=self.access_token, access_token_secret=self.access_token_secret)

    def request(self, config_params, request_params):
        assert "report" in config_params.keys(), "config_params must have a report key which can be UserTimeLine"
        assert "flatten" in config_params.keys(), "config_params must have a flatten key which can be True or False"
        assert "to_csv" in config_params.keys(), "config_params must have a to_csv key which can be True or False"
        assert "variables" in config_params.keys(), """config_params must have a variables key with the variables 
                                                            separated by commas"""

        report = config_params["report"]
        flatten = config_params["flatten"] == "True"
        to_csv = config_params["to_csv"] == "True"
        variables = config_params["variables"]

        if report == "UserTimeLine":
            assert "user_id" in request_params.keys(), """if report is UserTimeLine request_params must have a 
                                                            user_id key"""
            assert "count" in request_params.keys(), "if report is UserTimeLine request_params must have a count key"
            assert "include_rts" in request_params.keys(), """if report is UserTimeLine request_params must have a 
                                                                include_rts key"""
            assert "exclude_replies" in request_params.keys(), """if report is UserTimeLine request_params must have a 
                                                                            exclude_replies key"""

            response = self.api.GetUserTimeline(user_id = request_params["user_id"], count=request_params["count"],
                                                include_rts=request_params["include_rts"],
                                                exclude_replies=request_params["exclude_replies"])
            if flatten:
                response = pd.DataFrame([iterate_flatten(i.AsDict()) for i in response])
                variables = variables.split(",")
                response = response[variables]
            if to_csv:
                response = df_to_csv(response)
            return response

