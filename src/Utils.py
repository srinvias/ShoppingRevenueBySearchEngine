import src.MyCustomError as mcr
import json
import boto3
import pandas as pd
import re
from io import StringIO

class Utils(object):
    def __init__(self):
        print("Initiation Method")

    def get_s3filename_from_event(self , event):
        if 'Records' not in event:
                return ''
        for r in event.get('Records'):
            # track only objected created events
            if not ( ( r.get('eventName') == "ObjectCreated:Put" )  and ( 's3' in r ) ) : continue
            bucket  = r['s3']['bucket']['name']
            key =  r['s3']['object']['key']
            return bucket, key
    
    def readFilesFromS3(self , bucket , key):
        s3 = boto3.client('s3')
        s3_response = s3.get_object(Bucket=bucket, Key=key)
        print("Enter - readFilesFromS3")
        return s3_response
    
    def writeToS3(self , bucket , prefix , output_df):
        print("Write output to s3")
        from datetime import datetime
        now = datetime.now()
        dt_string = now.strftime("%Y-%m-%d")
        file_name = dt_string + '_SearchKeywordPerformance.tab'
        s3_path = prefix+'/'+file_name
        
        s3 = boto3.client("s3")
        csv_buf = StringIO()
        output_df.to_csv(csv_buf, header=True, index=False , sep='\t')
        csv_buf.seek(0)
        s3.put_object(Bucket=bucket, Body=csv_buf.getvalue(), Key=s3_path)
        
    
    def readInputdatatoPandasDataframe(self , s3_response):
        print("Enter - readInputdatatoPandasDataframe")
        status = s3_response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            print(f"Successful S3 get_object response. Status - {status}")
            input_df = pd.read_csv(s3_response.get("Body"), sep='\t')
            return input_df
        else:
            raise mcr.MyCustomError('Issue in reading S3 files')