import requests
from api_connectors.utils import get_yesterdays_date, select_variables_from_csv
from api_connectors.utils import get_logger


class AppsFlyer:
    def __init__(self, token):
        self.token = token
        self.logger = get_logger()

    def request(self, report, body):
        self.logger.info("Body: %s", body)
        body["api_token"] = self.token
        assert report in ["geo_by_date_report_android", "geo_by_date_report_ios", "geo_by_date_report_android_v2",
                          "geo_by_date_report_ios_v2", "raw_data_android_1","raw_data_ios_1","personalized",
                          "geo_by_date_report_ios_v3", "geo_by_date_report_android_v3"], \
            "The report is not implemented"
        if report == "geo_by_date_report_android":
            if body["from"] == "start_date":
                body["from"] = get_yesterdays_date(False)
            if body["to"] == "end_date":
                body["to"] = get_yesterdays_date(False)
            url = "https://hq.appsflyer.com/export/com.moviecity.app/geo_by_date_report/v5"
            try:
                data = requests.get(url, params=body).content
                self.logger.info("Data: %s", data)
                variables = ["Date", "Country", "Agency/PMD (af_prt)", "Media Source (pid)", "Campaign (c)",
                             "Impressions", "Clicks", "CTR", "Installs", "Conversion Rate", "Sessions", "Loyal Users",
                             "Loyal Users/Installs", "Total Revenue", "Total Cost", "ROI", "ARPU", "Average eCPI",
                             "af_login (Unique users)", "af_login (Event counter)", "af_login (Sales in USD)",
                             "af_try_and_buy (Unique users)", "af_try_and_buy (Event counter)",
                             "af_try_and_buy (Sales in USD)", "app error detected (Unique users)",
                             "app error detected (Event counter)", "app error detected (Sales in USD)",
                             "application installed (Unique users)", "application installed (Event counter)",
                             "application installed (Sales in USD)", "application opened (Unique users)",
                             "application opened (Event counter)", "application opened (Sales in USD)",
                             "application updated (Unique users)", "application updated (Event counter)",
                             "application updated (Sales in USD)", "inapp_valueproposition_start (Unique users)",
                             "inapp_valueproposition_start (Event counter)",
                             "inapp_valueproposition_start (Sales in USD)", "install attributed (Unique users)",
                             "install attributed (Event counter)", "install attributed (Sales in USD)",
                             "order completed (Unique users)", "order completed (Event counter)",
                             "order completed (Sales in USD)", "product added (Unique users)",
                             "product added (Event counter)", "product added (Sales in USD)"]
                data = select_variables_from_csv(data, variables)
                return data
            except Exception as e:
                self.logger.info("The error was: %s", e)
        elif report == "geo_by_date_report_ios":
            if body["from"] == "start_date":
                body["from"] = get_yesterdays_date(False)
            if body["to"] == "end_date":
                body["to"] = get_yesterdays_date(False)
            url = "https://hq.appsflyer.com/export/id650460795/geo_by_date_report/v5"
            try:
                data = requests.get(url, params=body).content
                self.logger.info("Data: %s", data)
                variables = ["Date", "Country", "Agency/PMD (af_prt)", "Media Source (pid)", "Campaign (c)",
                             "Impressions", "Clicks", "CTR", "Installs", "Conversion Rate", "Sessions", "Loyal Users",
                             "Loyal Users/Installs", "Total Revenue", "Total Cost", "ROI", "ARPU", "Average eCPI",
                             "af_login (Unique users)", "af_login (Event counter)", "af_login (Sales in USD)",
                             "af_purchase (Unique users)", "af_purchase (Event counter)", "af_purchase (Sales in USD)",
                             "af_try_and_buy (Unique users)", "af_try_and_buy (Event counter)",
                             "af_try_and_buy (Sales in USD)", "app error detected (Unique users)",
                             "app error detected (Event counter)", "app error detected (Sales in USD)",
                             "application installed (Unique users)", "application installed (Event counter)",
                             "application installed (Sales in USD)", "application opened (Unique users)",
                             "application opened (Event counter)", "application opened (Sales in USD)",
                             "application updated (Unique users)", "application updated (Event counter)",
                             "application updated (Sales in USD)", "inapp_valueproposition_start (Unique users)",
                             "inapp_valueproposition_start (Event counter)",
                             "inapp_valueproposition_start (Sales in USD)",  "install attributed (Unique users)",
                             "install attributed (Event counter)", "install attributed (Sales in USD)",
                             "product added (Unique users)", "product added (Event counter)",
                             "product added (Sales in USD)"]
                data = select_variables_from_csv(data, variables)
                return data
            except Exception as e:
                self.logger.info("The error was: %s", e)
        elif report == "geo_by_date_report_android_v2":
            if body["from"] == "start_date":
                body["from"] = get_yesterdays_date(False)
            if body["to"] == "end_date":
                body["to"] = get_yesterdays_date(False)
            url = "https://hq.appsflyer.com/export/com.moviecity.app/geo_by_date_report/v5"
            try:
                data = requests.get(url, params=body).content
                self.logger.info("Data: %s", data)
                variables = ["Date", "Country", "Agency/PMD (af_prt)", "Media Source (pid)", "Campaign (c)",
                             "Impressions", "Clicks", "CTR", "Installs", "Conversion Rate", "Sessions", "Loyal Users",
                             "Loyal Users/Installs", "Total Revenue", "Total Cost", "ROI", "ARPU", "Average eCPI",
                             "af_login (Unique users)", "af_login (Event counter)", "af_login (Sales in USD)",
                             "af_try_and_buy (Unique users)", "af_try_and_buy (Event counter)",
                             "af_try_and_buy (Sales in USD)", "app error detected (Unique users)",
                             "app error detected (Event counter)", "app error detected (Sales in USD)",
                             "application installed (Unique users)", "application installed (Event counter)",
                             "application installed (Sales in USD)", "application opened (Unique users)",
                             "application opened (Event counter)", "application opened (Sales in USD)",
                             "application updated (Unique users)", "application updated (Event counter)",
                             "application updated (Sales in USD)", "inapp_valueproposition_start (Unique users)",
                             "inapp_valueproposition_start (Event counter)",
                             "inapp_valueproposition_start (Sales in USD)", "install attributed (Unique users)",
                             "install attributed (Event counter)", "install attributed (Sales in USD)",
                             "order completed (Unique users)", "order completed (Event counter)",
                             "order completed (Sales in USD)", "product added (Unique users)",
                             "product added (Event counter)", "product added (Sales in USD)",
                             "order completed fa_br_m_52 (Uniqueusers)", "order completed fa_br_m_52 (Event counter)",
                             "order completed fa_br_m_52 (Sales in USD)", "order completed fa_br_m_5230dt (Unique users)",
                             "order completed fa_br_m_5230dt (Event counter)",
                             "order completed fa_br_m_5230dt (Sales in USD)", "product added fa_br_m_52 (Unique users)",
                             "product added fa_br_m_52 (Event counter)", "product added fa_br_m_52 (Sales in USD)",
                             "product added fa_br_m_5230dt (Unique users)", "product added fa_br_m_5230dt (Event counter)",
                             "product added fa_br_m_5230dt (Sales in USD)"]

                data = select_variables_from_csv(data, variables)
                return data
            except Exception as e:
                self.logger.info("The error was: %s", e)

        elif report == "geo_by_date_report_ios_v2":
            if body["from"] == "start_date":
                body["from"] = get_yesterdays_date(False)
            if body["to"] == "end_date":
                body["to"] = get_yesterdays_date(False)
            url = "https://hq.appsflyer.com/export/id650460795/geo_by_date_report/v5"
            try:
                data = requests.get(url, params=body).content
                self.logger.info("Data: %s", data)
                variables = ["Date", "Country", "Agency/PMD (af_prt)", "Media Source (pid)", "Campaign (c)",
                             "Impressions", "Clicks", "CTR", "Installs", "Conversion Rate", "Sessions", "Loyal Users",
                             "Loyal Users/Installs", "Total Revenue", "Total Cost", "ROI", "ARPU", "Average eCPI",
                             "af_login (Unique users)", "af_login (Event counter)", "af_login (Sales in USD)",
                             "af_purchase (Unique users)", "af_purchase (Event counter)", "af_purchase (Sales in USD)",
                             "af_try_and_buy (Unique users)", "af_try_and_buy (Event counter)",
                             "af_try_and_buy (Sales in USD)", "app error detected (Unique users)",
                             "app error detected (Event counter)", "app error detected (Sales in USD)",
                             "application installed (Unique users)", "application installed (Event counter)",
                             "application installed (Sales in USD)", "application opened (Unique users)",
                             "application opened (Event counter)", "application opened (Sales in USD)",
                             "application updated (Unique users)", "application updated (Event counter)",
                             "application updated (Sales in USD)", "inapp_valueproposition_start (Unique users)",
                             "inapp_valueproposition_start (Event counter)",
                             "inapp_valueproposition_start (Sales in USD)", "install attributed (Unique users)",
                             "install attributed (Event counter)", "install attributed (Sales in USD)",
                             "product added (Unique users)", "product added (Event counter)",
                             "product added (Sales in USD)", "order completed fa_br_m_52 (Unique users)",
                             "order completed fa_br_m_52 (Event counter)", "order completed fa_br_m_52 (Sales in USD)",
                             "order completed fa_br_m_5230dt (Unique users)",
                             "order completed fa_br_m_5230dt (Event counter)",
                             "order completed fa_br_m_5230dt (Sales in USD)", "product added fa_br_m_52 (Unique users)",
                             "product added fa_br_m_52 (Event counter)","product added fa_br_m_52 (Sales in USD)",
                             "product added fa_br_m_5230dt (Unique users)",
                             "product added fa_br_m_5230dt (Event counter)",
                             "product added fa_br_m_5230dt (Sales in USD)"]
                data = select_variables_from_csv(data, variables)
                return data
            except Exception as e:
                self.logger.info("The error was: %s", e)
        elif report == "raw_data_android_1":
            if body["from"] == "start_date":
                body["from"] = str(get_yesterdays_date(False)) + ' ' + body["from_time"]
            if body["to"] == "end_date":
                body["to"] = str(get_yesterdays_date(False)) + ' ' + body["to_time"]

            url = "https://hq.appsflyer.com/export/com.moviecity.app/in_app_events_report/v5"
            try:
                data = requests.get(url, params=body).content
                self.logger.info("Data: %s", data[0:100])
                variables = ['Attributed Touch Type', 'Attributed Touch Time', 'Install Time',
                               'Event Time', 'Event Name', 'Event Value', 'Event Revenue',
                               'Event Revenue Currency', 'Event Revenue USD', 'Event Source',
                               'Is Receipt Validated', 'Partner', 'Media Source', 'Channel',
                               'Keywords', 'Campaign', 'Campaign ID', 'Adset', 'Adset ID', 'Ad',
                               'Ad ID', 'Ad Type', 'Site ID', 'Sub Site ID', 'Sub Param 1',
                               'Sub Param 2', 'Sub Param 3', 'Sub Param 4', 'Sub Param 5',
                               'Cost Model', 'Cost Value', 'Cost Currency', 'Contributor 1 Partner',
                               'Contributor 1 Media Source', 'Contributor 1 Campaign',
                               'Contributor 1 Touch Type', 'Contributor 1 Touch Time',
                               'Contributor 2 Partner', 'Contributor 2 Media Source',
                               'Contributor 2 Campaign', 'Contributor 2 Touch Type',
                               'Contributor 2 Touch Time', 'Contributor 3 Partner',
                               'Contributor 3 Media Source', 'Contributor 3 Campaign',
                               'Contributor 3 Touch Type', 'Contributor 3 Touch Time', 'Region',
                               'Country Code', 'State', 'City', 'Postal Code', 'DMA', 'IP', 'WIFI',
                               'Operator', 'Carrier', 'Language', 'AppsFlyer ID', 'Advertising ID',
                               'IDFA', 'Android ID', 'Customer User ID', 'IMEI', 'IDFV', 'Platform',
                               'Device Type', 'OS Version', 'App Version', 'SDK Version', 'App ID',
                               'App Name', 'Bundle ID', 'Is Retargeting',
                               'Retargeting Conversion Type', 'Attribution Lookback',
                               'Reengagement Window', 'Is Primary Attribution', 'User Agent',
                               'HTTP Referrer', 'Original URL']

                data = select_variables_from_csv(data, variables, read_sep=",",write_sep=";")
                return data
            except Exception as e:
                self.logger.info("The error was: %s", e)

        elif report == "raw_data_ios_1":
            if body["from"] == "start_date":
                body["from"] = str(get_yesterdays_date(False)) + ' ' + body["from_time"]
            if body["to"] == "end_date":
                body["to"] = str(get_yesterdays_date(False)) + ' ' + body["to_time"]
            url = "https://hq.appsflyer.com/export/id650460795/in_app_events_report/v5"
            try:
                data = requests.get(url, params=body).content
                self.logger.info("Data: %s", data[0:100])
                variables = ["Attributed Touch Type", "Attributed Touch Time", "Install Time", "Event Time",
                             "Event Name", "Event Value", "Event Revenue", "Event Revenue Currency", "Event Revenue USD",
                             "Event Source", "Is Receipt Validated", "Partner", "Media Source", "Channel", "Keywords",
                             "Campaign", "Campaign ID", "Adset", "Adset ID", "Ad", "Ad ID", "Ad Type", "Site ID",
                             "Sub Site ID", "Sub Param 1", "Sub Param 2", "Sub Param 3", "Sub Param 4", "Sub Param 5",
                             "Cost Model", "Cost Value", "Cost Currency", "Contributor 1 Partner",
                             "Contributor 1 Media Source", "Contributor 1 Campaign", "Contributor 1 Touch Type",
                             "Contributor 1 Touch Time", "Contributor 2 Partner", "Contributor 2 Media Source",
                             "Contributor 2 Campaign", "Contributor 2 Touch Type", "Contributor 2 Touch Time",
                             "Contributor 3 Partner", "Contributor 3 Media Source", "Contributor 3 Campaign",
                             "Contributor 3 Touch Type", "Contributor 3 Touch Time", "Region", "Country Code", "State",
                             "City", "Postal Code", "DMA", "IP", "WIFI", "Operator", "Carrier", "Language",
                             "AppsFlyer ID", "Advertising ID", "IDFA", "Android ID", "Customer User ID", "IMEI", "IDFV",
                             "Platform", "Device Type", "OS Version", "App Version", "SDK Version", "App ID", "App Name",
                             "Bundle ID", "Is Retargeting", "Retargeting Conversion Type", "Attribution Lookback",
                             "Reengagement Window", "Is Primary Attribution", "User Agent", "HTTP Referrer",
                             "Original URL"]

                data = select_variables_from_csv(data, variables, read_sep=",", write_sep=";")

                return data
            except Exception as e:
                self.logger.info("The error was: %s", e)

        elif report == "geo_by_date_report_android_v3":
            if body["from"] == "start_date":
                body["from"] = get_yesterdays_date(False)
            if body["to"] == "end_date":
                body["to"] = get_yesterdays_date(False)
            url = "https://hq.appsflyer.com/export/com.moviecity.app/geo_by_date_report/v5"

            try:
                data = requests.get(url, params=body).content
                self.logger.info("Data: %s", data)
                variables = ["Date", "Country", "Agency/PMD (af_prt)", "Media Source (pid)", "Campaign (c)",
                             "Impressions", "Clicks", "CTR", "Installs", "Conversion Rate", "Sessions", "Loyal Users",
                             "Loyal Users/Installs", "Total Revenue", "Total Cost", "ROI", "ARPU", "Average eCPI",
                             "af_login (Unique users)", "af_login (Event counter)", "af_login (Sales in USD)",
                             "af_purchase (Unique users)", "af_purchase (Event counter)", "af_purchase (Sales in USD)",
                             "af_try_and_buy (Unique users)", "af_try_and_buy (Event counter)",
                             "af_try_and_buy (Sales in USD)", "app error detected (Unique users)",
                             "app error detected (Event counter)", "app error detected (Sales in USD)",
                             "application installed (Unique users)", "application installed (Event counter)",
                             "application installed (Sales in USD)", "application opened (Unique users)",
                             "application opened (Event counter)", "application opened (Sales in USD)",
                             "application updated (Unique users)", "application updated (Event counter)",
                             "application updated (Sales in USD)", "inapp_valueproposition_start (Unique users)",
                             "inapp_valueproposition_start (Event counter)",
                             "inapp_valueproposition_start (Sales in USD)", "install attributed (Unique users)",
                             "install attributed (Event counter)", "install attributed (Sales in USD)",
                             "product added (Unique users)", "product added (Event counter)",
                             "product added (Sales in USD)", "order completed fa_br_m_52 (Unique users)",
                             "order completed fa_br_m_52 (Event counter)", "order completed fa_br_m_52 (Sales in USD)",
                             "order completed fa_br_m_5230dt (Unique users)",
                             "order completed fa_br_m_5230dt (Event counter)",
                             "order completed fa_br_m_5230dt (Sales in USD)", "product added fa_br_m_52 (Unique users)",
                             "product added fa_br_m_52 (Event counter)", "product added fa_br_m_52 (Sales in USD)",
                             "product added fa_br_m_5230dt (Unique users)",
                             "product added fa_br_m_5230dt (Event counter)",
                             "product added fa_br_m_5230dt (Sales in USD)",
                             "order completed fa_br_m_52_2 (Unique users)",
                             "order completed fa_br_m_52_2 (Event counter)",
                             "order completed fa_br_m_52_2 (Sales in USD)",
                             "product added fa_br_m_52_2 (Unique users)", "product added fa_br_m_52_2 (Event counter)",
                             "product added fa_br_m_52_2 (Sales in USD)",
                             "order completed fa_br_m_527dt_2 (Unique users)",
                             "order completed fa_br_m_527dt_2 (Event counter)",
                             "order completed fa_br_m_527dt_2 (Sales in USD)",
                             "product added fa_br_m_527dt_2 (Unique users)",
                             "product added fa_br_m_527dt_2 (Event counter)",
                             "product added fa_br_m_527dt_2 (Sales in USD)",
                             "order completed dtcfull52_oct2017 (Unique users)",
                             "order completed dtcfull52_oct2017 (Event counter)",
                             "order completed dtcfull52_oct2017 (Sales in USD)",
                             "product added dtcfull52_oct2017 (Unique users)",
                             "product added dtcfull52_oct2017 (Event counter)",
                             "product added dtcfull52_oct2017 (Sales in USD)",
                             "product added fa_mx_m_52 (Unique users)",
                             "product added fa_mx_m_52 (Event counter)", "product added fa_mx_m_52 (Sales in USD)",
                             "order completed fa_mx_m_52 (Unique users)", "order completed fa_mx_m_52 (Event counter)",
                             "order completed fa_mx_m_52 (Sales in USD)",
                             "product added dtclite12_oct2017 (Unique users)",
                             "product added dtclite12_oct2017 (Event counter)",
                             "product added dtclite12_oct2017 (Sales in USD)",
                             "order completed dtclite12_oct2017 (Unique users)",
                             "order completed dtclite12_oct2017 (Event counter)",
                             "order completed dtclite12_oct2017 (Sales in USD)",
                             "order completed fa_car1_m_1230dt (Unique users)",
                             "order completed fa_car1_m_1230dt (Event counter)",
                             "order completed fa_car1_m_1230dt (Sales in USD)",
                             "product added fa_car1_m_1230dt (Unique users)",
                             "product added fa_car1_m_1230dt (Event counter)",
                             "product added fa_car1_m_1230dt (Sales in USD)",
                             "product added fa_car1_m_12 (Unique users)",
                             "product added fa_car1_m_12 (Event counter)",
                             "product added fa_car1_m_12 (Sales in USD)",
                             "order completed fa_car1_m_12 (Unique users)",
                             "order completed fa_car1_m_12 (Event counter)",
                             "order completed fa_car1_m_12 (Sales in USD)",
                             "product added fa_cam_m_52 (Unique users)",
                             "product added fa_cam_m_52 (Event counter)", "product added fa_cam_m_52 (Sales in USD)",
                             "order completed fa_cam_m_52 (Unique users)",
                             "order completed fa_cam_m_52 (Event counter)",
                             "order completed fa_cam_m_52 (Sales in USD)",
                             "order completed fa_cam_m_527dt (Unique users)",
                             "order completed fa_cam_m_527dt (Event counter)",
                             "order completed fa_cam_m_527dt (Sales in USD)",
                             "product added fa_cam_m_527dt (Unique users)",
                             "product added fa_cam_m_527dt (Event counter)",
                             "product added fa_cam_m_527dt (Sales in USD)",
                             "order completed fa_cam_m_12 (Unique users)",
                             "order completed fa_cam_m_12_2 (Event counter)",
                             "order completed fa_cam_m_12_2 (Sales in USD)",
                             "product added fa_cam_m_12_2 (Unique users)",
                             "product added fa_cam_m_12_2 (Event counter)",
                             "product added fa_cam_m_12_2 (Sales in USD)",
                             "order completed fa_cam_m_12_2 (Unique users)",
                             "order completed fa_cam_m_12_2 (Event counter)",
                             "order completed fa_cam_m_12_2 (Sales in USD)",
                             "product added fa_cam_m_12_2 (Unique users)",
                             "product added fa_cam_m_12_2 (Event counter)",
                             "product added fa_cam_m_12_2 (Sales in USD)", "product added fa_co_m_12 (Unique users)",
                             "product added fa_co_m_12 (Event counter)", "product added fa_co_m_12 (Sales in USD)",
                             "order completed fa_co_m_12 (Unique users)", "order completed fa_co_m_12 (Event counter)",
                             "order completed fa_co_m_12 (Sales in USD)", "product added fa_co_m_52 (Unique users)",
                             "product added fa_co_m_52 (Event counter)", "product added fa_co_m_52 (Sales in USD)",
                             "order completed fa_co_m_52 (Unique users)", "order completed fa_co_m_52 (Event counter)",
                             "order completed fa_co_m_52 (Sales in USD)", "product added fa_co_m_527dt (Unique users)",
                             "product added fa_co_m_527dt (Event counter)",
                             "product added fa_co_m_527dt (Sales in USD)",
                             "order completed fa_co_m_527dt (Unique users)",
                             "order completed fa_co_m_527dt (Event counter)",
                             "order completed fa_co_m_527dt (Sales in USD)", "product added fa_ec_m_12 (Unique users)",
                             "product added fa_ec_m_12 (Event counter)", "product added fa_ec_m_12 (Sales in USD)",
                             "order completed fa_ec_m_12 (Unique users)", "order completed fa_ec_m_12 (Event counter)",
                             "order completed fa_ec_m_12 (Sales in USD)", "product added fa_ec_m_52 (Unique users)",
                             "product added fa_ec_m_52 (Event counter)", "product added fa_ec_m_52 (Sales in USD)",
                             "order completed fa_ec_m_52 (Unique users)", "order completed fa_ec_m_52 (Event counter)",
                             "order completed fa_ec_m_52 (Sales in USD)", "product added fa_ec_m_527dt (Unique users)",
                             "product added fa_ec_m_527dt (Event counter)",
                             "product added fa_ec_m_527dt (Sales in USD)",
                             "order completed fa_ec_m_527dt (Unique users)",
                             "order completed fa_ec_m_527dt (Event counter)",
                             "order completed fa_ec_m_527dt (Sales in USD)",
                             "order completed fa_pe_m_12 (Unique users)",
                             "order completed fa_pe_m_12 (Event counter)", "order completed fa_pe_m_12 (Sales in USD)",
                             "product added fa_pe_m_12 (Unique users)", "product added fa_pe_m_12 (Event counter)",
                             "product added fa_pe_m_12 (Sales in USD)", "order completed fa_pe_m_527dt (Unique users)",
                             "order completed fa_pe_m_527dt (Event counter)",
                             "order completed fa_pe_m_527dt (Sales in USD)",
                             "product added fa_pe_m_527dt (Unique users)",
                             "product added fa_pe_m_527dt (Event counter)",
                             "product added fa_pe_m_527dt (Sales in USD)", "order completed fa_pe_m_52 (Unique users)",
                             "order completed fa_pe_m_52 (Event counter)", "order completed fa_pe_m_52 (Sales in USD)",
                             "product added fa_pe_m_52 (Unique users)", "product added fa_pe_m_52 (Event counter)",
                             "product added fa_pe_m_52 (Sales in USD)", "order completed fa_cl_m_12 (Unique users)",
                             "order completed fa_cl_m_12 (Event counter)", "order completed fa_cl_m_12 (Sales in USD)",
                             "product added fa_cl_m_12 (Unique users)", "product added fa_cl_m_12 (Event counter)",
                             "product added fa_cl_m_12 (Sales in USD)", "product added fa_cl_m_52 (Unique users)",
                             "product added fa_cl_m_52 (Event counter)", "product added fa_cl_m_52 (Sales in USD)",
                             "product added fa_cl_m_527dt (Unique users)",
                             "product added fa_cl_m_527dt (Event counter)",
                             "product added fa_cl_m_527dt (Sales in USD)", "order completed fa_cl_m_52 (Unique users)",
                             "order completed fa_cl_m_52 (Event counter)", "order completed fa_cl_m_52 (Sales in USD)",
                             "order completed fa_cl_m_527dt (Unique users)",
                             "order completed fa_cl_m_527dt (Event counter)",
                             "order completed fa_cl_m_527dt (Sales in USD)"]

                data = select_variables_from_csv(data, variables)
                return data
            except Exception as e:
                self.logger.info("The error was: %s", e)

        elif report == "geo_by_date_report_ios_v3":
            if body["from"] == "start_date":
                body["from"] = get_yesterdays_date(False)
            if body["to"] == "end_date":
                body["to"] = get_yesterdays_date(False)
            url = "https://hq.appsflyer.com/export/id650460795/geo_by_date_report/v5"
            try:
                data = requests.get(url, params=body).content
                self.logger.info("Data: %s", data)
                variables = ["Date", "Country", "Agency/PMD (af_prt)", "Media Source (pid)", "Campaign (c)",
                             "Impressions", "Clicks", "CTR", "Installs", "Conversion Rate", "Sessions", "Loyal Users",
                             "Loyal Users/Installs", "Total Revenue", "Total Cost", "ROI", "ARPU", "Average eCPI",
                             "af_login (Unique users)", "af_login (Event counter)", "af_login (Sales in USD)",
                             "af_try_and_buy (Unique users)", "af_try_and_buy (Event counter)",
                             "af_try_and_buy (Sales in USD)", "app error detected (Unique users)",
                             "app error detected (Event counter)", "app error detected (Sales in USD)",
                             "application installed (Unique users)", "application installed (Event counter)",
                             "application installed (Sales in USD)", "application opened (Unique users)",
                             "application opened (Event counter)", "application opened (Sales in USD)",
                             "application updated (Unique users)", "application updated (Event counter)",
                             "application updated (Sales in USD)", "inapp_valueproposition_start (Unique users)",
                             "inapp_valueproposition_start (Event counter)",
                             "inapp_valueproposition_start (Sales in USD)", "install attributed (Unique users)",
                             "install attributed (Event counter)", "install attributed (Sales in USD)",
                             "order completed (Unique users)", "order completed (Event counter)",
                             "order completed (Sales in USD)", "product added (Unique users)",
                             "product added (Event counter)", "product added (Sales in USD)",
                             "order completed fa_br_m_52 (Uniqueusers)", "order completed fa_br_m_52 (Event counter)",
                             "order completed fa_br_m_52 (Sales in USD)",
                             "order completed fa_br_m_5230dt (Unique users)",
                             "order completed fa_br_m_5230dt (Event counter)",
                             "order completed fa_br_m_5230dt (Sales in USD)", "product added fa_br_m_52 (Unique users)",
                             "product added fa_br_m_52 (Event counter)", "product added fa_br_m_52 (Sales in USD)",
                             "product added fa_br_m_5230dt (Unique users)",
                             "product added fa_br_m_5230dt (Event counter)",
                             "product added fa_br_m_5230dt (Sales in USD)",
                             "order completed fa_br_m_52_2 (Unique users)",
                             "order completed fa_br_m_52_2 (Event counter)",
                             "order completed fa_br_m_52_2 (Sales in USD)",
                             "product added fa_br_m_52_2 (Unique users)", "product added fa_br_m_52_2 (Event counter)",
                             "product added fa_br_m_52_2 (Sales in USD)",
                             "order completed fa_br_m_527dt_2 (Unique users)",
                             "order completed fa_br_m_527dt_2 (Event counter)",
                             "order completed fa_br_m_527dt_2 (Sales in USD)",
                             "product added fa_br_m_527dt_2 (Unique users)",
                             "product added fa_br_m_527dt_2 (Event counter)",
                             "product added fa_br_m_527dt_2 (Sales in USD)",
                             "order completed dtcfull52_oct2017 (Unique users)",
                             "order completed dtcfull52_oct2017 (Event counter)",
                             "order completed dtcfull52_oct2017 (Sales in USD)",
                             "product added dtcfull52_oct2017 (Unique users)",
                             "product added dtcfull52_oct2017 (Event counter)",
                             "product added dtcfull52_oct2017 (Sales in USD)",
                             "product added fa_mx_m_52 (Unique users)",
                             "product added fa_mx_m_52 (Event counter)", "product added fa_mx_m_52 (Sales in USD)",
                             "order completed fa_mx_m_52 (Unique users)", "order completed fa_mx_m_52 (Event counter)",
                             "order completed fa_mx_m_52 (Sales in USD)",
                             "product added dtclite12_oct2017 (Unique users)",
                             "product added dtclite12_oct2017 (Event counter)",
                             "product added dtclite12_oct2017 (Sales in USD)",
                             "order completed dtclite12_oct2017 (Unique users)",
                             "order completed dtclite12_oct2017 (Event counter)",
                             "order completed dtclite12_oct2017 (Sales in USD)",
                             "order completed fa_car1_m_1230dt (Unique users)",
                             "order completed fa_car1_m_1230dt (Event counter)",
                             "order completed fa_car1_m_1230dt (Sales in USD)",
                             "product added fa_car1_m_1230dt (Unique users)",
                             "product added fa_car1_m_1230dt (Event counter)",
                             "product added fa_car1_m_1230dt (Sales in USD)",
                             "product added fa_car1_m_12 (Unique users)",
                             "product added fa_car1_m_12 (Event counter)",
                             "product added fa_car1_m_12 (Sales in USD)",
                             "order completed fa_car1_m_12 (Unique users)",
                             "order completed fa_car1_m_12 (Event counter)",
                             "order completed fa_car1_m_12 (Sales in USD)",
                             "product added fa_cam_m_52 (Unique users)",
                             "product added fa_cam_m_52 (Event counter)", "product added fa_cam_m_52 (Sales in USD)",
                             "order completed fa_cam_m_52 (Unique users)",
                             "order completed fa_cam_m_52 (Event counter)",
                             "order completed fa_cam_m_52 (Sales in USD)",
                             "order completed fa_cam_m_527dt (Unique users)",
                             "order completed fa_cam_m_527dt (Event counter)",
                             "order completed fa_cam_m_527dt (Sales in USD)",
                             "product added fa_cam_m_527dt (Unique users)",
                             "product added fa_cam_m_527dt (Event counter)",
                             "product added fa_cam_m_527dt (Sales in USD)",
                             "order completed fa_cam_m_12 (Unique users)",
                             "order completed fa_cam_m_12_2 (Event counter)",
                             "order completed fa_cam_m_12_2 (Sales in USD)",
                             "product added fa_cam_m_12_2 (Unique users)",
                             "product added fa_cam_m_12_2 (Event counter)",
                             "product added fa_cam_m_12_2 (Sales in USD)",
                             "order completed fa_cam_m_12_2 (Unique users)",
                             "order completed fa_cam_m_12_2 (Event counter)",
                             "order completed fa_cam_m_12_2 (Sales in USD)",
                             "product added fa_cam_m_12_2 (Unique users)",
                             "product added fa_cam_m_12_2 (Event counter)",
                             "product added fa_cam_m_12_2 (Sales in USD)", "product added fa_co_m_12 (Unique users)",
                             "product added fa_co_m_12 (Event counter)", "product added fa_co_m_12 (Sales in USD)",
                             "order completed fa_co_m_12 (Unique users)", "order completed fa_co_m_12 (Event counter)",
                             "order completed fa_co_m_12 (Sales in USD)", "product added fa_co_m_52 (Unique users)",
                             "product added fa_co_m_52 (Event counter)", "product added fa_co_m_52 (Sales in USD)",
                             "order completed fa_co_m_52 (Unique users)", "order completed fa_co_m_52 (Event counter)",
                             "order completed fa_co_m_52 (Sales in USD)", "product added fa_co_m_527dt (Unique users)",
                             "product added fa_co_m_527dt (Event counter)",
                             "product added fa_co_m_527dt (Sales in USD)",
                             "order completed fa_co_m_527dt (Unique users)",
                             "order completed fa_co_m_527dt (Event counter)",
                             "order completed fa_co_m_527dt (Sales in USD)", "product added fa_ec_m_12 (Unique users)",
                             "product added fa_ec_m_12 (Event counter)", "product added fa_ec_m_12 (Sales in USD)",
                             "order completed fa_ec_m_12 (Unique users)", "order completed fa_ec_m_12 (Event counter)",
                             "order completed fa_ec_m_12 (Sales in USD)", "product added fa_ec_m_52 (Unique users)",
                             "product added fa_ec_m_52 (Event counter)", "product added fa_ec_m_52 (Sales in USD)",
                             "order completed fa_ec_m_52 (Unique users)", "order completed fa_ec_m_52 (Event counter)",
                             "order completed fa_ec_m_52 (Sales in USD)", "product added fa_ec_m_527dt (Unique users)",
                             "product added fa_ec_m_527dt (Event counter)",
                             "product added fa_ec_m_527dt (Sales in USD)",
                             "order completed fa_ec_m_527dt (Unique users)",
                             "order completed fa_ec_m_527dt (Event counter)",
                             "order completed fa_ec_m_527dt (Sales in USD)",
                             "order completed fa_pe_m_12 (Unique users)",
                             "order completed fa_pe_m_12 (Event counter)", "order completed fa_pe_m_12 (Sales in USD)",
                             "product added fa_pe_m_12 (Unique users)", "product added fa_pe_m_12 (Event counter)",
                             "product added fa_pe_m_12 (Sales in USD)", "order completed fa_pe_m_527dt (Unique users)",
                             "order completed fa_pe_m_527dt (Event counter)",
                             "order completed fa_pe_m_527dt (Sales in USD)",
                             "product added fa_pe_m_527dt (Unique users)",
                             "product added fa_pe_m_527dt (Event counter)",
                             "product added fa_pe_m_527dt (Sales in USD)", "order completed fa_pe_m_52 (Unique users)",
                             "order completed fa_pe_m_52 (Event counter)", "order completed fa_pe_m_52 (Sales in USD)",
                             "product added fa_pe_m_52 (Unique users)", "product added fa_pe_m_52 (Event counter)",
                             "product added fa_pe_m_52 (Sales in USD)", "order completed fa_cl_m_12 (Unique users)",
                             "order completed fa_cl_m_12 (Event counter)", "order completed fa_cl_m_12 (Sales in USD)",
                             "product added fa_cl_m_12 (Unique users)", "product added fa_cl_m_12 (Event counter)",
                             "product added fa_cl_m_12 (Sales in USD)", "product added fa_cl_m_52 (Unique users)",
                             "product added fa_cl_m_52 (Event counter)", "product added fa_cl_m_52 (Sales in USD)",
                             "product added fa_cl_m_527dt (Unique users)",
                             "product added fa_cl_m_527dt (Event counter)",
                             "product added fa_cl_m_527dt (Sales in USD)", "order completed fa_cl_m_52 (Unique users)",
                             "order completed fa_cl_m_52 (Event counter)", "order completed fa_cl_m_52 (Sales in USD)",
                             "order completed fa_cl_m_527dt (Unique users)",
                             "order completed fa_cl_m_527dt (Event counter)",
                             "order completed fa_cl_m_527dt (Sales in USD)"]

                data = select_variables_from_csv(data, variables)
                return data
            except Exception as e:
                self.logger.info("The error was: %s", e)
        elif report == "personalized":
            if body["from"] == "start_date":
                body["from"] = get_yesterdays_date(False)
            if body["to"] == "end_date":
                body["to"] = get_yesterdays_date(False)
            if ("from_time" in body.keys() and "to_time" in body.keys()):
                body["from"] = str(get_yesterdays_date(False)) + ' ' + body["from_time"]
                body["to"] = str(get_yesterdays_date(False)) + ' ' + body["to_time"]

            url = "https://hq.appsflyer.com/export/{}/{}/{}".format(body["app_id"],body["report_name"],body["version"])
            try:
                data = requests.get(url, params=body).content
                self.logger.info("Data: %s", data[0:100])
                variables = body["variables"].split(',')
                data = select_variables_from_csv(data, variables, read_sep=",", write_sep=";")
                return data
            except Exception as e:
                self.logger.info("The error was: %s", e)