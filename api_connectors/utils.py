import os
import time
from logging import Formatter, Logger, StreamHandler
import datetime
from io import StringIO
import pandas as pd
import numpy as np


def select_variables_from_csv(data, columns, read_sep=",", write_sep = ","):
    """
    Function that select a subset of columns from a csv in a string of type bytes
    :param data: csv of type bytes
    :param columns: list of variables to select
    :param sep: separator
    :param read_sep: separator to read
    :return: csv of type bytes
    """
    csv_buffer = StringIO()
    string_data = StringIO((data).decode("utf-8"))
    df = pd.read_csv(string_data, sep=read_sep, low_memory=False)
    del string_data
    df = df.loc[:, columns]
    df.to_csv(csv_buffer, index=False, header=True, sep=write_sep)
    return csv_buffer.getvalue()


# Function to capture logs
def get_logger():
    level = os.getenv("LOGGING_LEVEL", "DEBUG")

    message_format = "[%(asctime)s] [%(levelname)s] %(message)s"
    timestamp_format = "%Y-%m-%dT%H:%M:%SZ"

    formatter = Formatter(fmt=message_format, datefmt=timestamp_format)
    formatter.converter = time.gmtime

    handler = StreamHandler()
    handler.setFormatter(formatter)

    logger = Logger("migrator", level=level)
    logger.addHandler(handler)

    return logger


def get_time_range(as_string=True):
    root_date = get_yesterdays_date(False)
    start_date = datetime.datetime(root_date.year, root_date.month, root_date.day, 3, 0, 0, 0)
    end_date = start_date + datetime.timedelta(days=1)
    if as_string:
        return start_date.strftime("%Y-%m-%dT%H:%M:%SZ"), end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        return start_date, end_date


def flatten(data):
    """
    This function takes a dict and flatten it one level
    :param data: dict
    :return: dict
    """
    # initialize new empty dict
    new_data = {}
    for i in data.keys():
        if type(data[i]) is list:
            # Iterate through indexes
            for j in range(len(data[i])):
                new_data[i + "_" + str(j)] = data[i][j]
        elif type(data[i]) is dict:
            new_data_2 = flatten(data[i])
            for key, value in new_data_2.items():
                new_data[i + "_" + key] = value
        else:
            new_data[i] = data[i]
    return new_data


def iterate_flatten(data):
    """
    Iterate through a dict several times to flatten it completely
    :param data: dict
    :return: dict
    """
    completed = False
    while completed is False:
        count = 0
        data = flatten(data)
        for i in data.keys():
            if type(data[i]) is not dict and type(data[i]) is not list:
                count +=1
        if count == len(data.keys()):
            completed = True
    return data


def get_yesterdays_date(formatted=True):
    date = datetime.datetime.now().date() - datetime.timedelta(days=1)
    if formatted:
        date = str(date).replace("-", "")

    return date


def df_to_csv(df, header=True, sep="|", escapechar="\\", quotechar="'"):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, header=header, sep=sep, escapechar=escapechar, quotechar=quotechar)
    return csv_buffer.getvalue()


def to_tidy(data, col_estandar):
    """
    This function takes a dict and generates a tidy df
    :param data: dict
    :return: df
    """
    id_vars = []
    lista_dict = []
    for k in data:
        new_dict = {}
        for i in k.keys():
            if type(k[i]) is list:
                for j in range(len(k[i])):
                    new_dict[str(i + "_" + k[i][j]['action_type'])] = k[i][j]['value']
            elif type(k[i]) is dict:
                for keys, values in k[i].items():
                    new_dict[str(i + "_" + keys)] = values

            else:
                id_vars.append(str(i))
                new_dict[str(i)] = k[i]
        lista_dict.append(new_dict)

    df = pd.DataFrame.from_dict(data=lista_dict)
    if not id_vars:
        for i in col_estandar:
            df[i] = np.NaN
    else:
        id_vars = list(dict.fromkeys(id_vars))
        df = pd.melt(frame=df, id_vars=id_vars)
        if len(list(df)) < len(col_estandar):
            for i in col_estandar:
                if i not in list(df):
                    df[i] = np.NaN
        else:
            df = df[col_estandar]

    df = df.reindex(columns=col_estandar)

    return df
def process_other_data(df):  
    """
    This function takes a pandas Dataframe and process the Other_Data column to create new Columns 
    :param df: PandasDataframe
    :return: PandasDataframe
    """
    if('Other_Data' in df.columns):
        df['Other_Data']=df['Other_Data'].astype(str)
        #Other_Data= 'dc_pre=CJ6h7saen9oCFQ18wQodQHQHSA;gtm=d42;~oref=https://www.ticketek.com.ar/websource/purchase/success/DJEOP18/'
        SP=df.Other_Data.str.split(';',expand=False)#Separo;
        #Fila SP=['dc_pre=CJ6h7saen9oCFQ18wQodQHQHSA', 'gtm=d42', '~oref=https://www.ticketek.com.ar/websource/purchase/success/DJEOP18/']
        SP2=SP.map(lambda x:[tuple(y.split('=',1)) for y in x])#Transformo en lista de tuplas(key,value)
        #Fila SP2=[('dc_pre', 'CJ6h7saen9oCFQ18wQodQHQHSA'), ('gtm', 'd42'), ('~oref', 'https://www.ticketek.com.ar/websource/purchase/success/DJEOP18/')]
        SP3=SP2.map(lambda x:dict(x))#Transformo en lista de dicts
        #Fila SP3{'dc_pre': 'CJ6h7saen9oCFQ18wQodQHQHSA', 'gtm': 'd42', '~oref': 'https://www.ticketek.com.ar/websource/purchase/success/DJEOP18/'}
        dfod=pd.DataFrame(SP3.to_list())

        #us=dfod.columns[dfod.columns.str.startswith('u')] #Filtrar solo las columnas u

        df.drop(columns=['Other_Data'],axis=1,inplace=True)

        Final=pd.concat([df,dfod], axis=1)
        return Final
    else:
        print("El Dataframe no contiene la columna Other_Data")
        return df

class S3Buckets(object):
    REGION_NAME = "us-east-1"
    PAID_MEDIA = "ba-workspace-prd"
    PAID_MEDIA_DEV = "ba-datalake-paid-social-media"
    PAID_MEDIA_PRO = "s3-fdw-paid-and-social-media"
    SOURCE_CODE = "paid_social_media/src"
    PROJECT_PREFIX = "paid_social_media/"


class QueueConfig(object):
    REGION_NAME = "us-east-1"
    MAX_CONCURRENT_JOB_RUNS = 3


class AwsLambda(object):
    start_lambda_name = "ps_media_lambda_start"
    next_lambda_name = "ps_media_lambda_next_task"


class Ssm(object):
    CLUSTER_NAME = "arn:aws:ecs:us-east-1:264576910958:cluster/etls-integration"
    QUEUE_NAME = "SQSDBiIntegration.fifo"
    REPOSITORY_NAME = "paid_social_media_repository"
    SUBNET = "subnet-ab2338f1"
    TASK_DEFINITION_NAME = ""
    ACCESS_PARAMS = "ps_media_accesses"


class Config(object):
    SRC_FULL_LOCATION = "s3://%s/src" % S3Buckets.SOURCE_CODE
    ZIP_LOCATION = "s3://%s" % S3Buckets.SOURCE_CODE
