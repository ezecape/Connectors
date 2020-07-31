from facebook_business import FacebookSession, FacebookAdsApi
from facebook_business.adobjects.user import User
from facebook_business.adobjects.adaccount import AdAccount
from api_connectors.utils import iterate_flatten, df_to_csv, get_yesterdays_date, S3Buckets, get_logger, to_tidy
import pandas as pd
import requests
import json
from aws_services.ssm import ParameterStore


class FacebookAdsInsights:

    def __init__(self, access_params, api_version="v7.0"):
        self.logger = get_logger()
        assert "app_id" in access_params.keys(), "access_params must have a app_id key"
        assert "app_secret" in access_params.keys(), "access_params must have a app_secret key"
        assert "access_token" in access_params.keys(), "access_params must have a access_token key"
        self.logger.info("Test: Fecha ult. modificacion: 23/06/2020")
        self.app_id = access_params["app_id"]
        self.app_secret = access_params["app_secret"]
        self.access_token = access_params["access_token"]
        self.api_version = api_version
        self.session = FacebookSession(app_id=self.app_id, app_secret=self.app_secret,
                                       access_token=self.access_token)
        self.api = FacebookAdsApi(self.session, api_version=self.api_version)
        FacebookAdsApi.set_default_api(self.api)
        self.me = User(fbid="me")

    def get_user_accounts(self):
        return self.me.get_ad_accounts()

    def request(self, config_params, request_params):
        assert "account_id" in config_params.keys(), "config_params must have a account_id key"
        assert "fields" in request_params.keys(), "request_params must have a fields key of type list"
        assert type(request_params["fields"]) is list, "field must be list"
        assert "params" in request_params.keys(), "request_params must have a params key of type dict"
        assert type(request_params["params"]) is dict, "params must be dict"
        # assert "flatten" in config_params.keys(), "config_params must have a flatten key which can be True or False"
        assert "to_csv" in config_params.keys(), "config_params must have a to_csv key which can be True or False"
        assert "to_tidy" in config_params.keys(), "config_params must have a to_tidy key which can be True or False"
        assert "selector_cols" in config_params.keys(), "config_params must have a selector_cols key"
        # flatten = config_params["flatten"] == "True"
        to_csv = config_params["to_csv"] == "True"
        tidy = config_params["to_tidy"] == "True"

        response = AdAccount(config_params["account_id"]).get_insights(fields=request_params["fields"],
                                                                         params=request_params["params"])

        if tidy and to_csv:
            response = to_tidy([i.export_all_data() for i in response], config_params["selector_cols"])
            response = df_to_csv(response)
        elif tidy and not to_csv:
            response = to_tidy([i.export_all_data() for i in response], config_params["selector_cols"])
        else:
            None
        return response


class FacebookInsights:

    def __init__(self, access_params, api_version="v7.0"):
        self.logger = get_logger()
        self.number_of_params = int(access_params["number_of_params"])
        self.ssm_client = ParameterStore(region_name=S3Buckets.REGION_NAME)
        self.logger.info("Test: Fecha ult. modificacion: 23/06/2020")
        self.logger.info("ParameterStore client instantiated: %s", self.ssm_client)
        self.page_token = None
        self.root = "https://graph.facebook.com/"
        self.api_version = api_version

    def get_page_access(self, page_id):
        for i in range(self.number_of_params):
            accesses,version = self.ssm_client.get_parameter("fb_page_"+str(i+1))
            accesses = json.loads(accesses)
            if page_id in accesses.keys():
                self.page_token = accesses[page_id]
                break
            else:
                None
        if self.page_token is None:
            self.logger.info("Page token was not found!")

    def get_report_(self, report_type, config_params, request_params):
        assert report_type in ["posts_created", "network_report", "instagram_basics"], "report not implemented"
        if report_type == "posts_created":
            c_params = {}
            r_params = {"node_id": request_params["node_id"], "parameters": {"fields": "id,name,posts"},
                              "node_component": ""}
            try:
                response = self.standard_request_(c_params, r_params)
                result = json.loads(response.text)
                df = pd.DataFrame(result["posts"]["data"])
                df["node_id"] = request_params["node_id"]
                if "filter" in config_params.keys():
                    if config_params["filter"] == "yesterday":
                        df = df[pd.to_datetime(df["created_time"]).dt.date == get_yesterdays_date(False)]

                df["node_id"] = request_params["node_id"]
                df["date"] = get_yesterdays_date()
                df = df_to_csv(df)
            except KeyError:
                df = df_to_csv(pd.DataFrame(columns=["created_time",
                                                     "id",
                                                     "message", "story",
                                                     "node_id", "date"]))
            return df

        elif report_type == "network_report":
            c_params = {}
            r_params = {"node_id": request_params["node_id"], "parameters": {
                "metric": """page_fan_adds,page_impressions_unique,page_engaged_users,page_fans,
                page_consumptions_by_consumption_type_unique,page_posts_impressions_organic,page_posts_impressions_paid,
                page_posts_impressions_unique,page_posts_impressions_viral,page_fan_removes,page_impressions,
                page_video_views_organic,page_video_views,page_impressions_viral""",
                "period": "day", "date_preset": "yesterday"}, "node_component": "insights"}
            try:
                response = self.standard_request_(c_params, r_params)
                response = json.loads(response.text)
                result = {}
                for i in response["data"]:
                    result[i["name"]] = i["values"][0]["value"]
                df = pd.DataFrame(iterate_flatten(result), index=[0])
                df["node_id"] = request_params["node_id"]
                df["date"] = get_yesterdays_date()
                df = df_to_csv(df)
            except KeyError:
                df = df_to_csv(pd.DataFrame(
                    columns=["page_video_views_organic",
                             "page_video_views",
                             "page_fan_adds",
                             "page_impressions_unique",
                             "page_engaged_users",
                             "page_fans",
                             "page_posts_impressions_organic",
                             "page_posts_impressions_paid",
                             "page_posts_impressions_unique",
                             "page_posts_impressions_viral",
                             "page_fan_removes",
                             "page_impressions",
                             "page_impressions_viral",
                             "node_id",
                             "date"]))

            return df

        elif report_type == "instagram_basics":
            c_params = {}
            r_params = {"node_id": request_params["node_id"],
                        "parameters": {"fields": "instagram_accounts{follow_count,followed_by_count,media_count}",
                                       "period": "day", "date_preset": "yesterday"}, "node_component": ""}

            try:
                response = self.standard_request_(c_params, r_params)
                response = json.loads(response.text)
                df = pd.DataFrame(response["instagram_accounts"]["data"])
                df["node_id"] = request_params["node_id"]
                df["date"] = get_yesterdays_date()
                df = df_to_csv(df)
            except KeyError:
                df = df_to_csv(pd.DataFrame(columns=["follow_count",
                                                     "followed_by_count",
                                                     "id", "media_count",
                                                     "node_id", "date"]))
            return df

    def standard_request_(self, config_params, request_params):
        assert "node_id" in request_params.keys(), "request_params must have an account_id key"
        url = self.root + self.api_version + "/" + request_params["node_id"]
        if len(request_params["node_component"]) > 0:
            url += "/" + request_params["node_component"]
        if "parameters" in request_params.keys():
            url += "?"
            for parameter in request_params["parameters"].keys():
                url += parameter+"="+request_params["parameters"][parameter]+"&"
        url += "access_token="+self.page_token
        response = requests.get(url)
        return response

    def request(self, config_params, request_params):
        assert "request_type" in config_params.keys()
        if config_params["request_type"] == "personalized":
            response = self.standard_request_(config_params, request_params)
        elif config_params["request_type"] == "predefined":
            response = self.get_report_(config_params["report_type"], config_params, request_params)
        else:
            raise Exception("request_type must be either personalized or predefined")
        return response
