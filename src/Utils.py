import src.MyCustomError as mcr
import json
import boto3
import pandas as pd
import re
from io import StringIO
import logging
from datetime import datetime

class Utils(object):
    def __init__(self , logger):
        self.logger = logging.getLogger('RevenueFromSearchEngine')
        
    def readFilesFromS3(self , bucket , key):
        """
            Read s3 file content by using boto3 module
            ----------
            args : string ,string 
                bucket - s3 bucket name , key - s3 file file full name
            Returns
            -------
            dict
                s3 content in dict
        """        
        self.logger.info("Read S3file from - {0}/{1}".format(bucket , key))
        s3 = boto3.client('s3')
        s3_response = s3.get_object(Bucket=bucket, Key=key)
        self.logger.info("******* {0}".format(type(s3_response)))
        return s3_response
    
    def get_s3filename_from_event(self , event):
        """
            get S3 bucket names and file name from triggered event record
            by considering only  event name - put objectcreated
            Parameters
            ----------
            arg1 : json
                Triggered event record
            Returns
            -------
            tuple
                s3 bucket & key name
        """
        if 'Records' not in event:
                return ''
        for r in event.get('Records'):
            if not ( ( r.get('eventName') == "ObjectCreated:Put" )  and ( 's3' in r ) ) : continue
            bucket  = r['s3']['bucket']['name']
            key =  r['s3']['object']['key']
            return bucket, key
    
    def readInputdatatoPandasDataframe(self , s3_response):
        """
            Read s3 buffer io data to pandas dataframe csv functionality
            ----------
            args : dict
                s3_response  - s3 input content
            Returns
            -------
            pandas dataframe
        """
        self.logger.info("read Input data to pandas Pandas Dataframe")
        status = s3_response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            self.logger.info("Successful S3 get_object response. Status - {0}".format(status))
            input_df = pd.read_csv(s3_response.get("Body"), sep='\t')
            return input_df
        else:
            raise mcr.MyCustomError('Issue in reading S3 files')
            
    def getDomainAndSearchKey(self, input, lookingFor='domain'):
        """
            Function to derive domain & search keyword by using regex patterns
            Parameters
            ----------
            arg1 : string , string
                input - referrer field from input
                looking for - domain or searchkey to get required output
            Returns
            -------
            string
                domain or searchkey
        """
        m = re.search('(https|http)?://([A-Za-z_0-9.-]+)((\/search)?.*(\?|&)(q=|p=)([a-zA-Z\+0-9]+).*)*', input)
        if m:
            if lookingFor=='domain':
                return m.group(2)
            elif lookingFor=='searchkey':
                searchkey = m.group(7)
                return searchkey.replace("+"," ")
        else:
            raise mcr.MyCustomError('Issue in Regex to get domain or searchkey')
    
    def revenueFromProductList(self , product_list):
        """
            Function to get sum of total amount of each purchase event's product list
            Parameters
            ----------
            arg1 : list
                event's purchase list
            Returns
            -------
            float
                total sum of revenue of a purchase event
        """
        revenue = 0
        for each_product in product_list.split(","):
            revenue = revenue + int(each_product.split(";")[3])
        return revenue


    def writeToS3(self , bucket , output_df , prefix='prod/output'):
        """
            Write dataframe to csv and then upload to s3 bucket
            ----------
            args : string ,string , string
                bucket - s3 bucket name , output_df - pandas dataframe , prefix - s3 output key prefix
        """
        now = datetime.now()
        dt_string = now.strftime("%Y-%m-%d")
        file_name = dt_string + '_SearchKeywordPerformance.tab'
        s3_path = prefix+'/'+file_name
        self.logger.info("Write output to s3 - {0}".format(s3_path))
        
        s3 = boto3.client("s3")
        csv_buf = StringIO()
        output_df.to_csv(csv_buf, header=True, index=False , sep='\t')
        csv_buf.seek(0)
        s3.put_object(Bucket=bucket, Body=csv_buf.getvalue(), Key=s3_path)