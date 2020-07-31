import time
from twitter_ads.client import Client
from twitter_ads.http import Request
import json
import pandas as pd
import gzip
from requests_oauthlib import OAuth1
import requests
from api_connectors.utils import get_time_range, flatten
from io import StringIO


class TwitterAds:
    """ This class is used for requesting Twitter Ads API"""

    def __init__(self, access_params):
        assert "consumer_key" in access_params.keys(), "access_params must have a consumer_key"
        assert "consumer_secret" in access_params.keys(), "consumer_secret must have a consumer_key"
        assert "access_token" in access_params.keys(), "access_token must have a consumer_key"
        assert "access_token_secret" in access_params.keys(), "access_token_secret must have a consumer_key"
        self.consumer_key = access_params["consumer_key"]
        self.consumer_secret = access_params["consumer_secret"]
        self.access_token = access_params["access_token"]
        self.access_token_secret = access_params["access_token_secret"]
        self.client = Client(self.consumer_key, self.consumer_secret, self.access_token, self.access_token_secret)
        self.account_id = None
        self.account_data = None

    def set_account_id(self, account_id):
        self.account_id = account_id
        self.client.accounts(self.account_id)

    def get_account_data(self):
        assert self.account_id is not None, "you need to set account_id first using set_account_id()"
        url = "https://ads-api.twitter.com/6/accounts"
        auth = OAuth1(self.consumer_key, self.consumer_secret, self.access_token, self.access_token_secret)
        with requests.Session() as session:
            session.auth = auth
            session.headers = {'User-Agent': 'python-TwitterAPI', 'Accept-Encoding': 'gzip'}
            response = session.get(url)
        self.account_data = response
        return self.account_data

    def request(self, params):
        assert "resource" in params.keys(), "resource must be specified in the parameters dictionary"
        assert "method" in params.keys(), "method must be specified in the parameters dictionary"
        assert "date_range_type" in params.keys(), """
        date_range_type must be specified in the parameters dictionary, it can be 'yesterday' or 'other' in which case 
        'start_date' and 'end_date' are needed
        """
        assert self.account_id is not None, "account_id must be set before executing the request, use set_account_id"

        # Known reports can be accessed directly
        if params["resource"] == "stats":
            resource = "/6/stats/jobs/accounts/"
        else:
            resource = params["resource"]
        method = params["method"]

        if "account_id" not in params.keys():
            params["account_id"] = self.account_id

        resource = resource + params["account_id"]
        date_range_type = params["date_range_type"]

        # If one wants a report about yesterday's performance...
        if date_range_type == "yesterday":
            start_time, end_time = get_time_range()
            params["start_time"] = start_time
            params["end_time"] = end_time
        else:
            start_time = params["start_time"]
            end_time = params["end_time"]

        # Drop keys
        params.pop("resource", None)
        params.pop("method", None)
        params.pop("date_range_type", None)

        # TwitterAds API is consumed asynchronously, which means that first we need to post a report...
        response = Request(self.client, method, resource, params=params).perform()
        status = 'PROCESSING'
        count = 0

        # ... and then we need to check if it's finished requesting the status
        while status == 'PROCESSING':
            time.sleep(5)
            response = Request(self.client, 'get', resource, params=params).perform()
            status = response.body['data'][0]['status']
            if status == "SUCCESS":
                data_response = response.body['data'][0]

                # When finished, we get from another URL a zip file.
                url_gzip = data_response['url']
                response = Request(self.client, 'get', resource, params=params).perform()
                data_response = response.body['data'][0]
                status = data_response['status']
                url_gzip = data_response['url']

                r = requests.get(url_gzip, allow_redirects=True)
                resp = gzip.decompress(r.content).decode()

                data = json.loads(resp)['data'][0]["id_data"][0]["metrics"]

                # The response needs to be flattened, so as to get a plain text
                f_data = flatten(data)
                df = pd.DataFrame(f_data, index=[0])
                df["start_time"] = start_time
                df["end_time"] = end_time
                df["account_id"] = self.account_id
                df["entity"] = params["entity"]
                if "entity_ids" in params.keys():
                    df["entity_ids"] = params["entity_ids"]
                else:
                    df["entity_ids"] = "S/D"

                csv_buffer = StringIO()
                df.to_csv(csv_buffer, index=False, header=True)
                return csv_buffer.getvalue()

            elif count > 15:
                raise Exception("Report status never got SUCCESS")
            else:
                count += 1
