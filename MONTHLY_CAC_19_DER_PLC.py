#2022-03-08 update naming conventions
#2022-03-11 update files name ref DAG
#2022-03-12 redesign
import json
import requests
from datetime import datetime, date, timedelta,timezone
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine
from urllib.parse import quote
import vaex
import psycopg2 
from calendar import monthrange
import io
import sys
from io import StringIO, BytesIO
import boto3
import glob
import fnmatch
import glob
import pysftp
from common_function import function
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.email_operator import EmailOperator
from airflow.exceptions import AirflowFailException
from airflow.operators.dummy_operator import DummyOperator


#------------------------mocking data----------------------------
#Minio path = Test_vaex_airflow_KTB_m  -- > Test_vaex_airflow_KTB
#DB2 table 1.9 rdtdba.der_plc_test, 1.1  rdtdba.der_cac_test

def CAC19PLC_LOADLOAN_FCT01():
    #dowload only config.txt
    function.dowload_config_file('monthly-cac-19-der-plc')

    #read config and get object key value {'Load_path': 'load/'}
    config = function.read_config()

    bucket = function.create_connection_minio(config['bucket_name'])
    path = config['Load_path']
    file_name = config['file_name01']
    try:
        #read table idv,convert dataframe to .parquet and upload to minio
        df,con_db2 = function.read_db2_monthly(config['RDT010901'],config['MSR_PRD_ID'])
        function.df_to_parquet_and_check_data(df,file_name)
        function.upload_files_to_minio(file_name,path,bucket)
        con_db2.close()
    except Exception as e:
        print(e)
        print('Connection db2 fail,cannot upload files to minio')
        raise AirflowFailException("Connection db2 fail,cannot upload files to minio")


def CAC19PLC_XFRM01():
    #function mapping polcy classification
    def mapping_polcy(polcy,covid_f,sl_f,loan_tp_id,awh_code,drbiz_code):
        if polcy != None:
            if covid_f == 'N':
                return "2003800099"
            elif loan_tp_id  >= 1100 and loan_tp_id <= 4201 :
                return '2003800003'
            elif covid_f == 'Y':
                return "2003800002"
            elif sl_f=='1':
                return '2003800001'
            elif awh_code == 'AWH01':
                return '2003800004'
            elif drbiz_code == 'DR1' or drbiz_code == 'DR1':
                return '2003800005'    
        else :
            return polcy


    #dowload only config.txt
    function.dowload_config_file('monthly-cac-19-der-plc')

    #read config and get object key value {'Load_path': 'load/'}
    config = function.read_config()

    try:
        #dowload table from minio
        bucket = function.create_connection_minio(config['bucket_name'])
        function.dowload_parquet_from_minio(config['Load_path'],bucket)
    except Exception as e:
        print(e)
        print('Connect minio fail, cannot dowload file from minio')
        raise AirflowFailException("Connect minio fail, cannot dowload file from minio")

    #vaex read table idv and org get dataframe
    df  = vaex.open('{}*.parquet'.format(config['file_name01'])) # load data

    #change type to string
    df['eff_dt'] = df.eff_dt.astype('str')
    df['end_dt'] = df.end_dt.astype('str')
    df['othr_polcy_dsc'] = df.othr_polcy_dsc.astype('str')

    column_df = ['ac_id','eff_dt','end_dt', 'polcy', 'othr_polcy_dsc','loan_tp_id','covid_f','sl_f','drbiz_code','awh_code']

    #clear space ans set space = null
    df = function.clear_space(df,column_df)

    #--------------------polcy fix --------------------------------------------------
    #create column polcy
    df['loan_tp_id'] = df.loan_tp_id.astype('int64')
    df['polcy']= df.apply(mapping_polcy, arguments=[df.polcy,df.covid_f,df.sl_f,df.loan_tp_id,df.awh_code,df.drbiz_code]).values
    df=df['ac_id','eff_dt','end_dt', 'polcy', 'othr_polcy_dsc']

    #create column
    date_month = function.data_dt('monthly')
    msr_month = config['MSR_PRD_ID']
    org_id = config['org_id']
    df['msr_prd_id'] = np.repeat(msr_month, df.shape[0])
    df['org_id'] = np.repeat(org_id, df.shape[0])
    df['data_dt'] = np.repeat(date_month, df.shape[0])
    df['ppn_tm'] = np.repeat(function.data_dt('timestamp'), df.shape[0])

    #select column
    df_new = df[['msr_prd_id','org_id', 'data_dt', 'ac_id', 'eff_dt', 'end_dt', 'polcy','othr_polcy_dsc','ppn_tm']]

    #fill null dataframe
    df_new['msr_prd_id'] = df_new['msr_prd_id'].fillna(value=msr_month)
    df_new['org_id'] = df_new['org_id'].fillna(value=org_id)
    df_new['data_dt'] = df_new['data_dt'].fillna(value=date_month)
    df_new['ac_id'] = df_new['ac_id'].fillna(value='XXXXXXXXXX')
    df_new['eff_dt'] = df_new['eff_dt'].fillna(value=(date(9999,9,9)).isoformat())
    df_new['end_dt'] = df_new['end_dt'].fillna(value=(date(9999,9,9)).isoformat())
    df_new['polcy'] = df_new['polcy'].fillna(value='XXXXXXXXXX')
    df_new['othr_polcy_dsc'] = df_new['othr_polcy_dsc'].fillna(value='XXXXXXXXXXXXX')

    #upload table to minio
    name = config['file_name02']
    df_new.export_many(name+'_{i:03}.parquet',chunk_size=int(config['chunksize']))                
    function.upload_files_to_minio(name,config['fxrm_path'],bucket)


def CAC19PLC_MPI_DER_PLC():
    function.dowload_config_file('monthly-cac-19-der-plc','config/config.txt')
    config = function.read_config()

    try:
        #dowload table from minio
        bucket = function.create_connection_minio(config['bucket_name'])
        function.dowload_parquet_from_minio(config['fxrm_path'],bucket)
    except Exception as e:
        print(e)
        print('Connect minio fail, cannot dowload file from minio')
        raise AirflowFailException("Connect minio fail, cannot dowload file from minio")

    table = config['table']
    chunksize = int(config['chunksize'])

    #insert table to PostgreSQL    
    query = function.query_insert(table)
    try:
        #check offset from PostgreSQL
        offset_index = function.check_offset_from_postgresql(table,chunksize)

        function.insert_table_into_postgresql(config['file_name03'],query,offset_index,chunksize)
    except Exception as e:
        print(e)
        print('cannot insert data to PostgreSQL')
        raise AirflowFailException("cannot insert data to PostgreSQL")


def CAC19PLC_VALD01(): #rdtdba.der_cac_test (1.1) , rdtdba.der_plc_test (1.9)
    # query data with condition RDT vilidation and return Account ID where not True 
    def CNPLC001(conn): # eff_dt at table 1.9 must more than or equal to table 1.1
        query = open(config['RDT010902'], 'rt')
        df = pd.read_sql(query.read(), conn) 
        query.close()
        return df

    def CNPLC002(conn): #column end_dt must more than or equal column eff_dt
        query = open(config['RDT010903'], 'rt')
        df = pd.read_sql(query.read(), conn) 
        query.close()
        return df

    def CMPLC001(conn): #othr_polcy_dsc must have a value if polcy has a value ‘2003800099’
        query = open(config['RDT010904'], 'rt')
        df = pd.read_sql(query.read(), conn) 
        query.close()
        return df

    def RIPLC001(conn):  #column AC_ID in table 1.9 must be in table 1.1
        query = open(config['RDT010905'], 'rt')
        df = pd.read_sql(query.read(), conn) 
        query.close()
        return df
        
    #dowload only config.txt    
    function.dowload_config_file('monthly-cac-19-der-plc')

    #read config and get object key value {'Load_path': 'load/'}
    config = function.read_config()

    conn_psql = function.create_connection_psql()
    bucket_name = config['bucket_name']
    s3 = function.create_connection_minio(bucket_name,'s3')
    chunksize = int(config['chunksize'])
    name = config['file_name04']
    try:
        #validation rule get report column ac_id
        df,con = function.read_psql(config['RDT010902'])
        function.df_to_parquet(df,name)
        df,con = function.read_psql(config['RDT010903'])
        function.df_to_parquet(df,name)
        df,con = function.read_psql(config['RDT010904'])
        function.df_to_parquet(df,name)
        df,con = function.read_psql(config['RDT010905'])
        function.df_to_parquet(df,name)
        result_CNPLC001 = CNPLC001(conn_psql)
        result_CNPLC002 = CNPLC002(conn_psql)
        result_CMPLC001 = CMPLC001(conn_psql)
        result_RIPLC001 = RIPLC001(conn_psql)
        con.close()
        conn_psql.close()
    except Exception as e:
        print(e)
        print('Connection PostgreSQL fail')
        raise AirflowFailException("Connection PostgreSQL fail")



    #validation success
    if (result_CNPLC001['Account Id'].count() == 0 and 
        result_CNPLC002['Account Id'].count() == 0 and
        result_CMPLC001['Account Id'].count() == 0 and
        result_RIPLC001['Account Id'].count() == 0 ):
        print('success')
    else:
        unixtime = str(int(datetime.now().timestamp()))
        if result_CNPLC001['Account Id'].count() != 0:
            function.upload_csv_to_minio(result_CNPLC001,'{0}{1}/CAC19PLC_CNPLC001.csv'.format(config['vald_path'],unixtime),chunksize,s3,bucket_name)
        if result_CNPLC002['Account Id'].count() != 0:
            function.upload_csv_to_minio(result_CNPLC001,'{0}{1}/CAC19PLC_CNPLC002.csv'.format(config['vald_path'],unixtime),chunksize,s3,bucket_name)
        if result_CMPLC001['Account Id'].count() != 0:
            function.upload_csv_to_minio(result_CNPLC001,'{0}{1}/CAC19PLC_CMPLC001.csv'.format(config['vald_path'],unixtime),chunksize,s3,bucket_name)
        if result_RIPLC001['Account Id'].count() != 0:
            function.upload_csv_to_minio(result_CNPLC001,'{0}{1}/CAC19PLC_RIPLC001.csv'.format(config['vald_path'],unixtime),chunksize,s3,bucket_name)
        raise AirflowFailException("Data Validation Fail")


def CAC19PLC_GEN01():
    #dowload only config.txt and query.sql file
    function.dowload_config_file('monthly-cac-19-der-plc')

    #read config and get object key value {'Load_path': 'load/'}
    config = function.read_config()
    bucket_name = config['bucket_name']
    bucket = function.create_connection_minio(bucket_name)
    s3 = function.create_connection_minio(bucket_name,'s3')

    #load table from PostgreSQL and upload file .csv to minio
    path = '{0}{1}.csv'.format(config['gen_path'],config['MSR_PRD_ID'])

    try:
        df,conn_psql = function.read_psql(config['RDT010906'])
        df = pd.concat(df,ignore_index=True)
        function.upload_csv_to_minio(df,path, int(config['chunksize']),s3,bucket_name)
        conn_psql.close()
    except Exception as e:
        print(e)
        print('Connection PostgreSQL fail, cannot upload table to minio')
        raise AirflowFailException("Connection PostgreSQL fail, cannot upload table to minio")
        
    msr_prd_id = function.update_msr_prd_id_monthly(config['MSR_PRD_ID'])
    function.update_config_2(msr_prd_id,bucket,s3,bucket_name)  


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 2, 17),
    'schedule_interval': None,
}
with DAG('MONTHLY_CAC_19_DER_PLC_2',
         schedule_interval='@monthly', 
         default_args=default_args,
         description='Monthly: 1.9 Policy Adoption',
         catchup=False) as dag:
 
    t1 = PythonOperator(
        task_id='CAC19PLC_LOADLOAN_FCT01',
        python_callable=CAC19PLC_LOADLOAN_FCT01
    )

    t2 = PythonOperator(
        task_id='CAC19PLC_XFRM01',
        python_callable=CAC19PLC_XFRM01    
    )

    t3 = PythonOperator(
        task_id='CAC19PLC_MPI_DER_PLC',
        python_callable=CAC19PLC_MPI_DER_PLC      
    ) 

    t4 = PythonOperator(
        task_id='CAC19PLC_VALD01',
        python_callable=CAC19PLC_VALD01      
    ) 

    t5 = PythonOperator(
        task_id='CAC19PLC_GEN01',
        python_callable=CAC19PLC_GEN01
    )

    
    t1 >> t2 >> t3 >> t4 >> t5
