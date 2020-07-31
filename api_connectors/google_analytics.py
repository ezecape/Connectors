from google_api_toolkit.google_analytics import job as j, report as r, connection as c
from aws_services.ssm import ParameterStore
from api_connectors.utils import S3Buckets, get_yesterdays_date
import pandas as pd
from io import StringIO


class GoogleAnalytics:
    """
    This class is used for executing a request to YouTube Analytics API - V.2
    """
    def __init__(self, key_location, connection_id):
        self.key_location = key_location
        self.connection_id = connection_id
        self.get_parameter_()
        self.job = ''
        self.conn = c.Connection()
        self.conn.config_service(key_location)
        self.report = r.Report()

    def get_parameter_(self):
        ssm_client = ParameterStore(region_name=S3Buckets.REGION_NAME)
        param,version = ssm_client.get_parameter("ga_secret_" + self.connection_id)
        with open(self.key_location, "w") as f:
            f.write(param)

    def parse_elements_(self, params, element):
        elements_to_add = params[element]
        if element == "dimensions":
            [self.report.add_dimension(i) for i in elements_to_add.split(",")]
        if element == "metrics":
            [self.report.add_metric(i) for i in elements_to_add.split(",")]
        if element == "filters":
            [self.report.add_filter(i) for i in elements_to_add.split(",")]
        if element == "view_id":
            self.report.set_view_id(elements_to_add)

    def request(self, params):
        assert isinstance(params, dict), "params is not a dict"
        start_date = "S/D"
        end_date = "S/D"

        for i in params.keys():
            if i in ["dimensions","metrics","filters", "view_id"]:
                self.parse_elements_(params, i)
            if i == "date_range":
                assert "type" in params["date_range"].keys(), "type not in date_range keys"
                if params["date_range"]["type"] == "yesterday":
                    yesterday_date = str(get_yesterdays_date(formatted=False))
                    start_date = yesterday_date
                    end_date = yesterday_date
                    self.report.add_date_range(start_date, end_date)
                else:
                    assert "start_date" in params["date_range"].keys(), "start_date not in date_range keys"
                    assert "end_date" in params["date_range"].keys(), "end_date not in date_range keys"
                    start_date = params["date_range"]["start_date"]
                    end_date = params["date_range"]["end_date"]
                    self.report.add_date_range(start_date, end_date)
        self.report.build_report_body()
        self.job = j.Job(self.conn, self.report)
        response = self.job.execute()
        csv_buffer = StringIO()
        response.response_body["start_date"] = start_date
        response.response_body["end_date"] = end_date
        response.response_body.to_csv(csv_buffer, index=False, header=True)
        return csv_buffer.getvalue()
