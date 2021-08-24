
"""
============================================================================================================
RevenueFromSearchEngine.py
It is lambda function to process for every s3 triggered put object event
and get output of search engine , search keyword and corresponding total revenue in descending order
load ouput to s3 bucket with [YYYY-MM-dd]_SearchKeywordPerformance.tab
=============================================================================================================
History
=============================================================================================================
 Date           Author                  Desc
-------------------------------------------------------------------------------------------------------------
 2021-08-021   Srinivasarao Padala     New Script Created
============================================================================================================
"""

import json
import boto3
import pandas as pd
import re
from io import StringIO
import logging

import src.Utils as utl
import src.MyCustomError as mcr

logger = logging.getLogger('RevenueFromSearchEngine')
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

def main(event, context):
    """
        lambda handler function to read each in put file from s3 and process them to find out search engine & keyword and corresponding total revenue
        Parameters
        ----------
        arg1 : json , json
            event  - contains input data info & metadata of triggered event
            context - provides methods and properties that provide information about the invocation, function, and runtime environment
        Returns
        -------
        json
            status of the each triggered/processed event
    """
    logger.info("Entry point for RevenuFromSearchEngine process")
    utils_fns = utl.Utils(logger)

    #remove unnecessary columns
    bucket, s3_filename = utils_fns.get_s3filename_from_event(event)
    logger.info("Bucket name - {0}".format(bucket))
    logger.info("new s3 file name - {0}".format(s3_filename))
    
    if s3_filename =='':
        logger.error("There is no S3 create or put event happend")
        return {
        'statusCode': 501,
        'body': json.dumps('No S3 Create event is happend')
    }
    else:
        # Read file content from S3 bucket & key
        s3_response = utils_fns.readFilesFromS3(bucket , s3_filename)
        
        #Load input data to pandas dataframe
        hitdata_df = utils_fns.readInputdatatoPandasDataframe(s3_response)
        
        logger.info("Schema of input data - ")
        hitdata_df.info()
        logger.info("Input Sample data - ")
        hitdata_df.sample()
        
        

    #drop unrequired fields from dataframes
    hitdata_df.drop(['date_time','user_agent','geo_city','geo_region','geo_country','pagename'],axis = 1 , inplace=True)
    
    #derive page_url_domain and referrer_domain fields
    hitdata_df['page_url_domain'] = hitdata_df['page_url'].transform(utils_fns.getDomainAndSearchKey)
    hitdata_df['referrer_domain'] = hitdata_df['referrer'].transform(utils_fns.getDomainAndSearchKey)
    
    #filter and if page_url_domain and referrer_domain same  or filter event_list is not purchase event
    browse_and_purchase_df = hitdata_df.loc[(hitdata_df['event_list'] == 1) | (hitdata_df['page_url_domain'] != hitdata_df['referrer_domain'])]
    
    #divide events into groups basedon ip & domain part of page_url
    group_data_by_user_and_app = browse_and_purchase_df.groupby(["ip","page_url_domain"])
    
    revenue_by_searchkey_list = []
    for each_user_app, per_user_events_df in group_data_by_user_and_app:
        ip = each_user_app[0]
        page_url_domain = each_user_app[1]
        
        #derive 2 dataframes - 1st for purchase events and 2nd for search events 
        purchased_events_df = per_user_events_df.loc[per_user_events_df['event_list'] == 1]
        search_events_df = per_user_events_df.loc[per_user_events_df['event_list'] != 1]
        
        if purchased_events_df.shape[0] == 0:
            logger.info("There is no purchase event for  ip - {0}  , page_url - {1} ".format(ip  , page_url_domain))
            continue
        elif search_events_df.shape[0] == 0 and purchased_events_df.shape[0] == 1:
            logger.info("Search and purchase were happended with in the shopping website  - "+page_url_domain)
            continue
        
        #Iterate through purchase events and find nearest search event for purchase event - Note : for loop helps if there are multiple purchase events for that domain
        for index, row in purchased_events_df.iterrows():
            total_revenue = 0
            search_engine = ''
            search_key = ''
            pe_hit_time_gmt = row["hit_time_gmt"]
            pe_product_list = row["product_list"]
            
            #get search events which are happend before purchase & second condition is to check search event should have same page url like purchase event -  & (search_events_df['page_url_domain'] == pe_page_url_domain)
            search_events_df  = search_events_df[search_events_df['hit_time_gmt']<pe_hit_time_gmt]
            
            # find nearest search event for purchase event -- needs to test
            minidx = (pe_hit_time_gmt - search_events_df['hit_time_gmt']).idxmin()
            nearest_search_df = search_events_df.loc[[minidx]]
            
            #get search engine , search keyword and revenue
            search_engine = utils_fns.getDomainAndSearchKey(input = nearest_search_df.loc[nearest_search_df.index[0], 'referrer'] , lookingFor='domain')
            search_key = utils_fns.getDomainAndSearchKey(input = nearest_search_df.loc[nearest_search_df.index[0], 'referrer'] , lookingFor='searchkey')
            total_revenue = utils_fns.revenueFromProductList(pe_product_list)
            
            revenue_by_searchkey_list.append({'Search Engine Domain':search_engine.lower() , 'Search Keyword':search_key.lower() , 'Revenue':total_revenue})
    
    searchKeyRevenue_df = pd.DataFrame(revenue_by_searchkey_list)
    
    #get total revenue for each and search engine and corresponding keyword
    output_df = searchKeyRevenue_df[searchKeyRevenue_df.Revenue>0].groupby(['Search Engine Domain', 'Search Keyword'] , as_index=False)["Revenue"].agg('sum').sort_values(by='Revenue', ascending=False)
    
    #save output to S3
    utils_fns.writeToS3(bucket , output_df)
    
    return {
        'statusCode': 200,
        'body': json.dumps("Revenue From Search engine - process has been completed")
    }
