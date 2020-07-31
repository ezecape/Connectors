"""
This module function as a wrapper to the googleads package
"""
from googleads import adwords

class AdWords:
    """
    This class centralizes the authentication and request of Adwords reports
    """
    def __init__(self, credentials):
        assert isinstance(credentials, dict), "credentials is not a dict"
        self.credentials = credentials
        self.yaml = None
        self.client = None

    def make_client(self, client_customer_id):
        """
        This function instantiates an Adwords client
        :param credentials: dict with the following keys needed to authenticate: developer_token, client_customer_id,
        client_id, client_secret, refresh_token
        :return: None
        """
        self.yaml = """adwords:
                    developer_token: {0}
                    client_customer_id: {1}
                    client_id: {2}
                    client_secret: {3}
                    refresh_token: {4}""".format(self.credentials["developer_token"],
                                                 client_customer_id,
                                                 self.credentials["client_id"],
                                                 self.credentials["client_secret"],
                                                 self.credentials["refresh_token"])
        self.client = adwords.AdWordsClient.LoadFromString(self.yaml)

    def request(self, report_name, request_params):
        """
        This function takes a dictionary with the report configuration and makes a request to Adwords api
        :param report_name: string
        :param request_params: dict
        :return: str, report
        """
        assert "dateRangeType" in request_params.keys(), """
        config_params should have a date_range_type key with the date range to be requested. 
        It could be: TODAY, YESTERDAY, LAST_7_DAYS, LAST_WEEK, THIS_MONTH, LAST_MONTH, ALL_TIME, etc.
        """
        assert "reportType" in request_params.keys(), """
        config_params should have a report_type key with the report to be requested. 
        """
        assert "downloadFormat" in request_params.keys(), """
        config_params should have a download_format key with the format, for example CSV. 
        """
        assert "selector" in request_params.keys(), """
        config_params should have a selector key with a dictionary with the variables to be used. 
        """
        report = request_params
        report["reportName"] = report_name
        report_downloader = self.client.GetReportDownloader(version='v201809')
        results = report_downloader.DownloadReportAsStream(
            report, skip_report_header=True, skip_column_header=False, skip_report_summary=True,
            include_zero_impressions=False)
        results = str(results.read(), encoding="utf8")
        return results