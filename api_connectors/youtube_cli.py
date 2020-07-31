"""
This script is used for querying YouTube API by the command line. It expects to find credentials on src/
It returns a byte string in .csv format
"""
# -*- coding: utf-8 -*-
# !/usr/bin/python

import pandas as pd
from io import StringIO

import os
from googleapiclient.discovery import build
from oauth2client.tools import argparser, run_flow
from datetime import datetime, timedelta
import httplib2

from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from api_connectors.utils import get_logger

API_SCOPES = ["https://www.googleapis.com/auth/youtube.readonly",
              "https://www.googleapis.com/auth/yt-analytics.readonly"]

path = ''
API_SERVICE_NAME = 'youtubeAnalytics'
API_VERSION = 'v2'
CLIENT_SECRETS_FILE = os.path.join(path, 'yt_client_secret.json')


def get_authenticated_services(args, logger):
    flow = flow_from_clientsecrets(CLIENT_SECRETS_FILE, scope=" ".join(API_SCOPES))
    logger.info("flow: %s", flow)
    storage = Storage("%s.dat" % args.fox_channel_name)
    logger.info("storage: %s", storage)
    credentials = storage.get()
    logger.info("credentials: %s", credentials)
    if credentials is None or credentials.invalid:
        logger.info("starting flow...")
        credentials = run_flow(flow, storage, args)
    http = credentials.authorize(httplib2.Http())
    logger.info("http: %s", http)
    youtube_analytics = build(API_SERVICE_NAME, API_VERSION, http=http)
    logger.info("youtube_analytics: %s", youtube_analytics)
    return youtube_analytics


def execute_api_request(client_library_function, **kwargs):
    response = client_library_function(**kwargs).execute()
    return response


def process_result(results, args):
    column_names = [i["name"] for i in results["columnHeaders"]]
    df = pd.DataFrame([i for i in results["rows"]], columns=column_names)
    df["channel_name"] = args.fox_channel_name
    df["start_date"] = args.start_date
    if args.dimensions == "video":
        df["url"] = "https://www.youtube.com/watch?v=" + df.video
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, header=True)
    return csv_buffer.getvalue()


if __name__ == '__main__':
    logger = get_logger()
    now = datetime.now()
    one_day_ago = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    two_days_ago = (now - timedelta(days=2)).strftime("%Y-%m-%d")
    one_week_ago = (now - timedelta(days=7)).strftime("%Y-%m-%d")
    os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '0'
    argparser.add_argument("--channel_id", help="Channel ID",
                           required=True)
    argparser.add_argument("--content_owner_id", help="Content Owner Id",
                           required=False)
    argparser.add_argument("--metrics", help="Report metrics",
                           default="views,comments,likes,dislikes,shares")
    argparser.add_argument("--filters", help="Filters",
                           default="")

    argparser.add_argument("--dimensions", help="Report dimensions",
                           default="day")
    argparser.add_argument("--sort", help="Sort order", default="-views")
    argparser.add_argument("--fox_channel_name", help="Sort order", default="-canalfoxla")
    argparser.add_argument("--max_results", help="Max results", default=10)
    argparser.add_argument("--start_date", default=two_days_ago,
                           help="Start date, in YYYY-MM-DD format")
    argparser.add_argument("--end_date", default=one_day_ago,
                           help="End date, in YYYY-MM-DD format")

    args = argparser.parse_args()
    logger.info("args: %s", args)
    youtube_analytics = get_authenticated_services(args, logger)
    logger.info("get_authenticated_services(args): %s", youtube_analytics)

    results = execute_api_request(
        youtube_analytics.reports().query,
        ids='channel==%s' % args.channel_id,
        startDate='%s' % args.start_date,
        endDate='%s' % args.end_date,
        metrics='%s' % args.metrics,
        dimensions='%s' % args.dimensions,
        sort='%s' % args.sort,
        filters='%s' % args.filters,
        maxResults=args.max_results)

    final_result = process_result(results, args)
    print(final_result)
