import csv
import json
import boto3
import time
from datetime import datetime


def lambda_handler(event, context):
    
    event_params=event["Records"][0]

    bucket=event_params["s3"]["bucket"]["name"]
    key=event_params["s3"]["object"]["key"]
    
    print("here")
    print(bucket, key)
    
    s3_resource = boto3.resource('s3')
    s3_object = s3_resource.Object(bucket, key)
    
    data = s3_object.get()['Body'].read().decode('utf-8').splitlines()
    
    lines = csv.reader(data)
    print(lines)
    headers = next(lines)
    print('headers: %s' %(headers))
    
    list_data = list(lines)
    
    print(list_data)
    India=[]
    US=[]
    for i in list_data:
        if i[3]=='India':
            India.append(int(i[2]))
        else:
            US.append(int(i[2]))
        
    print('Total India Salary Spend is ',sum(India))
    print('Total US Salary Spend is ',sum(US))
    print(f"""Total India, US Salary Spend is ,{sum(India),sum(US)}""")
    file_content=f"""Total India, US Salary Spend is ,{sum(India),sum(US)}"""
    if key=='employee3.csv':
        s3 = boto3.client('s3') 
        s3.put_object(Body=file_content, Bucket=bucket, Key='agg')
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }