"""
Class that work as interface to the different APIs
"""
import api_connectors.appsflyer as af
import api_connectors.adwords as aw
import api_connectors.youtube as yt
import api_connectors.google_analytics as ga
import api_connectors.tw_ads as tw_ads
import api_connectors.tw as tw
import api_connectors.fb_insights as fbi
import api_connectors.google_dv360 as dv
from api_connectors.utils import get_logger
import api_connectors.google_bigquery as gbq
import api_connectors.tw_adsv2 as tw_adsv2


class Connect:
    def __init__(self, api, access_params):
        self.logger = get_logger()
        self.logger.info("api: %s", api)
        self.api = api
        self.access_params = access_params
        self.apis_list = ["appsflyer", "adwords", "youtube", "google_analytics", "twitter_ads", "twitter",
                          "facebook_ads", "facebook_page_posts","twitter_adsv2","google_dv360"]
        assert self.api in self.apis_list, "API name is not in list"
        assert isinstance(self.access_params, dict), "access_params is not a dict"
        if len(self.access_params.keys()) == 0:
            self.logger.info("access_params is empty")

    def execute(self, params):
        assert "request_params" in params.keys(), """
                    params should have a request_params key with the params needed to make the request
                    """
        assert "config_params" in params.keys(), """
                    params should have a config_params key with the params the params needed in the configuration
                    """

        request_params = params["request_params"]
        self.logger.info("request_params: %s", request_params)
        config_params = params["config_params"]
        self.logger.info("config_params: %s", config_params)

        assert isinstance(config_params, dict), "config_params is not a dict"
        assert isinstance(request_params, dict), "request_params is not a dict"

        if len(config_params.keys()) == 0:
            self.logger.info("config_params is empty")
        if len(request_params.keys()) == 0:
            self.logger.info("request_params is empty")

        if self.api == "appsflyer":
            assert "report" in config_params.keys(), """
            config_params must have a report key with the report to be requested
            """
            try:
                api = af.AppsFlyer(self.access_params["api_token"])
                self.logger.info("AppsFlyer object: %s", api)
                response = api.request(config_params["report"], request_params)
                self.logger.info("AppsFlyer response: %s", response[0:100])
                return response
            except KeyError as key_error:
                self.logger.info("access_params should have a api_token key with the token required")
                self.logger.info("Error: %s", key_error)

        if self.api == "adwords":
            assert "report_name" in config_params.keys(), """
            config_params must have a report_name key with the report's name. Although required by the
            API it's not used to make the request. 
            """
            "This parameter is included to maintain consistency of the API"
            try:
                api = aw.AdWords(self.access_params)
                self.logger.info("AdWords object: %s", api)
                api.make_client(config_params["client_customer_id"])
                self.logger.info("AdWords object after making client: %s", api)
                response = api.request(config_params["report_name"], request_params)
                self.logger.info("AdWords response: %s", response)
                return response
            except Exception as e:
                self.logger.info('There is a problem when executing AdWords: %s', e)
                raise Exception('There is a problem when executing AdWords:', e)

        if self.api == "youtube":
            assert "channel_id" in config_params.keys(), """
            config_params must have a channel_id 
            """
            assert "channel_name" in config_params.keys(), """
            config_params must have a channel_name which identifies the Fox channel name and is used to get the .dat 
            file
            """
            try:
                api = yt.YouTube()
                self.logger.info('YouTube object: %s', api)
                response = api.request(config_params, request_params)
                self.logger.info('YouTube response: %s', response)
                return response

            except Exception as e:
                self.logger.info('There is a problem when executing YouTube: %s', e)
                raise Exception('There is a problem when executing YouTube:', e)

        if self.api == "google_analytics":
            assert "key_location" in self.access_params.keys(), """
            access_params must have a key_location 
            """
            assert "connection_id" in config_params.keys(), """
            config_params must have a connection_id 
            """
            try:
                api = ga.GoogleAnalytics(self.access_params["key_location"], config_params["connection_id"])
                self.logger.info('GoogleAnalytics object: %s', api)
                response = api.request(request_params)
                self.logger.info('GoogleAnalytics response: %s', response)
                return response

            except Exception as e:
                self.logger.info('There is a problem when executing GoogleAnalytics: %s', e)
                raise Exception('There is a problem when executing GoogleAnalytics:', e)

        if self.api == "twitter_ads":
            try:
                api = tw_ads.TwitterAds(self.access_params)
                self.logger.info('TwitterAds object: %s', api)
                api.set_account_id(config_params["account_id"])
                response = api.request(request_params)
                self.logger.info('TwitterAds response: %s', response)
                return response

            except Exception as e:
                self.logger.info('There is a problem when executing TwitterAds: %s', e)
                raise Exception('There is a problem when executing TwitterAds:', e)
        if self.api == "twitter_adsv2":
            try:
                api = tw_adsv2.TwitterAdsv2(self.access_params)
                self.logger.info('TwitterAdsv2 object: %s', api)
                api.set_account_id(config_params["account_id"])
                response = api.request(request_params)
                self.logger.info('TwitterAdsv2 response: %s', response)
                return response

            except Exception as e:
                self.logger.info('There is a problem when executing TwitterAdsv2: %s', e)
                raise Exception('There is a problem when executing TwitterAdsv2:', e)

        if self.api == "twitter":
            try:
                api = tw.Twitter(self.access_params)
                self.logger.info('Twitter object: %s', api)
                response = api.request(config_params, request_params)
                self.logger.info('Twitter response: %s', response)
                return response

            except Exception as e:
                self.logger.info('There is a problem when executing Twitter: %s', e)
                raise Exception('There is a problem when executing Twitter:', e)

        if self.api == "facebook_ads":
            try:
                api = fbi.FacebookAdsInsights(self.access_params)
                self.logger.info('Facebook Ads Insights object: %s', api)
                response = api.request(config_params, request_params)
                self.logger.info('Facebook Ads Insights response: %s', response)
                return response

            except Exception as e:
                self.logger.info('There is a problem when executing Facebook Ads Insights: %s', e)
                raise Exception('There is a problem when executing Facebook Ads Insights:', e)

        if self.api == "facebook_page_posts":
            try:
                api = fbi.FacebookInsights(self.access_params)
                self.logger.info('Facebook Ads Insights object: %s', api)

                api.get_page_access(request_params["node_id"])
                response = api.request(config_params, request_params)
                self.logger.info('Facebook Ads Insights response: %s', response)
                return response
            except Exception as e:
                self.logger.info('There is a problem when executing Facebook Page and Post Insights: %s', e)
                raise Exception('There is a problem when executing Facebook Page and Post Insights:', e)
        if self.api == "googlebigquery":
            try:
                api = gbq.GoogleBigQuery(self.access_params)
                self.logger.info('GoogleBigQuery object: %s', api)
                response = api.request(config_params, request_params)
                self.logger.info('GoogleBigQuery response: %s', response)
                return response

            except Exception as e:
                self.logger.info('There is a problem when executing GoogleBigQuery: %s', e)
                raise Exception('There is a problem when executing GoogleBigQuery:', e)
        if self.api == "google_dv360":
            try:
                api = dv.Dv360()
                self.logger.info('GoogleDV360 object: %s', api)
                response = api.request(config_params, request_params)
                self.logger.info('GoogleDV360 response: %s', response)
                return response
            except Exception as e:
                self.logger.info('There is a problem when executing GoogleDV360: %s', e)
                raise Exception('There is a problem when executing GoogleDV360:', e)