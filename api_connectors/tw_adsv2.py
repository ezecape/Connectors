import time
import json
import pandas as pd
from io import StringIO

from twitter_ads.client import Client
from twitter_ads.account import Account
from twitter_ads.campaign import Campaign
from twitter_ads.utils import split_list
from datetime import datetime, timedelta

class TwitterAdsv2:
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


    def request(self, params):
        assert "date_range_type" in params.keys(), """
        date_range_type must be specified in the parameters dictionary, it can be 'yesterday' or 'other' in which case 
        'start_date' and 'end_date' are needed
        """
        assert self.account_id is not None, "account_id must be set before executing the request, use set_account_id"

        if "account_id" not in params.keys():
            params["account_id"] = self.account_id

        date_range_type = params["date_range_type"]

        # If one wants a report about yesterday's performance...
        if date_range_type == "yesterday":
            start_time = datetime.today()-timedelta(days=1)
            end_time = datetime.today()
        else:
            start_time = datetime.strptime(params["start_time"], "%Y-%m-%d %H:%M:%S")
            end_time = datetime.strptime(params["end_time"], "%Y-%m-%d %H:%M:%S")

        # Drop keys
        params.pop("date_range_type", None)

        account = self.client.accounts(params["account_id"])
        campaigns = account.campaigns()
        #Verifico Campañas activas
        camp_actives=[]
        for c in campaigns:
            if('EXPIRED' not in c.reasons_not_servable):
                camp_actives.append(c.id)
        
        DTFinal=pd.DataFrame(columns=['impressions', 'tweets_send', 'billed_charge_local_micro',
                                'qualified_impressions', 'follows', 'app_clicks', 'retweets',
                                'unfollows', 'likes', 'engagements', 'clicks', 'card_engagements',
                                'poll_card_vote', 'replies', 'url_clicks', 'billed_engagements',
                                'carousel_swipes', 'date', 'account_id', 'entity', 'entity_ids','entity_status','name'])
        
        if(not camp_actives):
            raise Exception("Report has only expired campaigns.")

        for campaing_id in camp_actives:
            campaigns = account.campaigns(campaing_id)
            
            # the list of metrics we want to fetch, for a full list of possible metrics
            # see: https://dev.twitter.com/ads/analytics/metrics-and-segmentation
            metric_groups = params['metric_groups'].split(',')

            # fetching stats on the instance
            campaigns.stats(metric_groups)

            sync_data = []
            sync_data.append(Campaign.all_stats(account, [campaing_id], metric_groups,
                                        start_time = start_time,
                                        end_time=end_time,
                                        granularity=params['granularity'],
                                        placement=params['placement'])
                                    )

            # create async stats jobs and get job ids
            queued_job_ids = []
            queued_job_ids.append(Campaign.queue_async_stats_job(account,
                                        [campaing_id],
                                        metric_groups,
                                        start_time = start_time,
                                        end_time=end_time,
                                        granularity=params['granularity'],
                                        placement=params['placement']).id)

            # let the job complete
            seconds = 30
            time.sleep(seconds)

            async_stats_job_results = Campaign.async_stats_job_result(account, job_ids=queued_job_ids)
            
            async_data = []
            for result in async_stats_job_results:
                async_data.append(Campaign.async_stats_job_data(account, url=result.url))


            
            dt_aux=pd.DataFrame(pd.DataFrame(async_data[0]['data'][0]['id_data'][0]['metrics'],columns=async_data[0]['data'][0]['id_data'][0]['metrics'].keys()))
            dt_aux["account_id"] = params['config_params']['account_id']
            dt_aux["entity"] = 'CAMPAIGN'
            dt_aux["entity_ids"] = campaing_id
            dt_aux['entity_status']=campaigns.entity_status
            dt_aux['name']=campaigns.name
            
            DTFinal=pd.concat([DTFinal,dt_aux], axis=0, ignore_index=True,sort=False)

        csv_buffer = StringIO()
        DTFinal.to_csv(csv_buffer, index=False, header=True)
        return csv_buffer.getvalue()
