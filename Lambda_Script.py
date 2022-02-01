#----www.prisoft.com----#
#----Date-01-02-2020----#
#----AWS lambda code to run Redshift Queries for Incremental Load----#

import json
import sys
import boto3
import awswrangler as wr
import pandas as pd
import urllib.parse

def lambda_handler(event, context):
    #-----Get the object from the event-----#
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    objecturi="s3://"+bucket+"/"+key

    #-----Function to Run SQL Statement using Data Wrangler-----#
    def run_sql_query(statement):
        con = wr.redshift.connect(secret_id=db_creds)
        with con.cursor() as cursor:
            try:
                cursor.execute(statement)
                row = cursor.fetchone()
                if row:
                    return row[0]
                return None
            except Exception as e:
                print(e)

            finally:
                con.commit()
                con.close()

    #-----Copy to Redshift Staging-----#
    bucket = 'pr***********et'
    file = 'S3toQuick**********script/sql/redshift_stage.sql'
    db_creds= "re*********er"
    parameters=[objecturi,"arn:aws:iam::761****************fts3access","us-east-2"]

    #get sql statements#
    s3 = boto3.client('s3') 
    sqls = s3.get_object(Bucket=bucket, Key=file)['Body'].read().decode('utf-8')
    sqls = sqls.split(';')

    #Run each sql statement#
    for sql in sqls[:-1]:
        sql = sql + ';'
        sql = sql.format(parameters[0],parameters[1],parameters[2]).strip('\n')
        print(sql)
        run_sql_query(sql)
    
    print("Task Finished")
    return { 
        'statusCode': 200,
        'body': "Task Ended"
    }
