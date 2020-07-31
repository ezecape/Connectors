"""
This script is used for querying Google DV360 API by the command line. It expects to find credentials on src/
It returns a byte string in .csv format
"""
# -*- coding: utf-8 -*-
# !/usr/bin/python

import pandas as pd

import os
from googleapiclient.discovery import build
from oauth2client.tools import argparser, run_flow
import httplib2

from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from api_connectors.utils import get_logger

from io import BytesIO
import time
from six.moves.urllib.request import urlopen
from contextlib import closing
from api_connectors.utils import df_to_csv

from unidecode import unidecode

API_SCOPES = ['https://www.googleapis.com/auth/doubleclickbidmanager']

path = ''
API_SERVICE_NAME = 'doubleclickbidmanager'
API_VERSION = 'v1.1'
CLIENT_SECRETS_FILE = os.path.join(path, 'google_dv360_client_secret.json')


def get_authenticated_services(args,logger):
    flow = flow_from_clientsecrets(CLIENT_SECRETS_FILE, scope=" ".join(API_SCOPES))
    logger.info("flow: %s", flow)
    storage = Storage("google_dv360_store_file.dat")
    logger.info("storage: %s", storage)
    credentials = storage.get()
    logger.info("credentials: %s", credentials)

    if credentials is None or credentials.invalid:
        
        logger.info("starting flow...")
        credentials = run_flow(flow, storage, args)

    http = credentials.authorize(httplib2.Http())
    logger.info("http: %s", http)
    resource = build(API_SERVICE_NAME, API_VERSION, http=http)
    logger.info("google_dv360: %s", resource)
    return resource

if __name__ == '__main__':
    logger = get_logger()
    os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '0'
    argparser.add_argument("--query_id", help="Query ID",
                           required=True)
    argparser.add_argument("--bad_lines", help="Exclude Bad lines",
                           required=True)
    args = argparser.parse_args()

    logger.info("args: %s", args)
    
    google_dv360_resource = get_authenticated_services(args,logger)

    logger.info("get_authenticated_services(args): %s", google_dv360_resource)

    google_dv360_resource.queries().runquery(queryId=args.query_id).execute()

    time.sleep(10)

    query = (google_dv360_resource.queries().getquery(queryId=args.query_id).execute())        

    report_url = query['metadata']['googleCloudStoragePathForLatestReport']

    with closing(urlopen(report_url)) as url:
        result_bytes=BytesIO(url.read())
    
    df_result=pd.read_csv(result_bytes,error_bad_lines=False)

    df_result=df_result.drop(df_result.tail(int(args.bad_lines)).index)

    response = df_to_csv(df_result)

    print(unidecode(response))

