# - RevenueFromSearchEngine.py
import json
import boto3
import pandas as pd
import re
from io import StringIO
import src.Utils as utl
import src.MyCustomError as mcr

# Pending - 
# maintains classes
# logger modules
# document desction
    
def getDomainAndSearchKey(input, lookingFor='domain'):
    m = re.search('(https|http)?://([A-Za-z_0-9.-]+)((\/search)?.*(\?|&)(q=|p=)([a-zA-Z\+0-9]+).*)*', input)
    if m:
        if lookingFor=='domain':
            return m.group(2)
        elif lookingFor=='searchkey':
            searchkey = m.group(7)
            return searchkey.replace("+"," ")
    else:
        raise mcr.MyCustomError('Issue in Regex to get domain or searchkey')

def revenueFromProductList(product_list):
    revenue = 0
    for each_product in product_list.split(","):
        revenue = revenue + int(each_product.split(";")[3])
    return revenue
    

def main(event, context):
    print("Entry")
    utils_fns = utl.Utils()
    print("RevenueFromSearchEngine - lambda")
    # TODO implement
    #remove unnecessary columns
    bucket, s3_filename = utils_fns.get_s3filename_from_event(event)
    print("****s3_filename **** - "+s3_filename)
    
    if s3_filename =='':
        return {
        'statusCode': 501,
        'body': json.dumps('No S3 Create event is happend')
    }
    else:
        s3_response = utils_fns.readFilesFromS3(bucket , s3_filename)
        hitdata_df = utils_fns.readInputdatatoPandasDataframe(s3_response)

    
    hitdata_df.drop(['date_time','user_agent','geo_city','geo_region','geo_country','pagename'],axis = 1 , inplace=True)
    
    #derive page_url_domain and referrer_domain fields
    hitdata_df['page_url_domain'] = hitdata_df['page_url'].transform(getDomainAndSearchKey)
    hitdata_df['referrer_domain'] = hitdata_df['referrer'].transform(getDomainAndSearchKey)
    
    #filter and if page_url_domain and referrer_domain same  or filter event_list is not purchase event
    browse_and_purchase_df = hitdata_df.loc[(hitdata_df['event_list'] == 1) | (hitdata_df['page_url_domain'] != hitdata_df['referrer_domain'])]
    
    #divide events into groups basedon ip & domain part of page_url
    group_data_by_user_and_app = browse_and_purchase_df.groupby(["ip","page_url_domain"])
    
    revenue_by_searchkey_list = []
    total_revenue = 0
    search_engine = ''
    search_key = ''
    for each_user_app, per_user_events_df in group_data_by_user_and_app:
        ip = each_user_app[0]
        page_url_domain = each_user_app[1]
        
        #derive 2 dataframes - 1st for purchase events and 2nd for search events 
        purchased_events_df = per_user_events_df.loc[per_user_events_df['event_list'] == 1]
        search_events_df = per_user_events_df.loc[per_user_events_df['event_list'] != 1]
        
        if purchased_events_df.shape[0] == 0:
            print("There is no purchase event for  ip - "+ip + "page_url - "+page_url_domain)
            continue
        elif search_events_df.shape[0] == 0 and purchased_events_df.shape[0] == 1:
            print("Search and bought product with in the shopping website  - "+page_url_domain)
            continue
        
        #Iterate through purchase events and find nearest search event for purchase event - Note : for loop helps if there are multiple purchase events for that domain
        total_revenue = 0
        search_engine = ''
        search_key = ''
        for index, row in purchased_events_df.iterrows():
            pe_hit_time_gmt = row["hit_time_gmt"]
            pe_product_list = row["product_list"]
            
            #get search events which are happend before purchase & second condition is to check search event should have same page url like purchase event -  & (search_events_df['page_url_domain'] == pe_page_url_domain)
            search_events_df  = search_events_df[search_events_df['hit_time_gmt']<pe_hit_time_gmt]
            
            # find nearest search event for purchase event -- needs to test
            minidx = (pe_hit_time_gmt - search_events_df['hit_time_gmt']).idxmin()
            nearest_search_df = search_events_df.loc[[minidx]]
            
            #get search engine , search keyword and revenue
            search_engine = getDomainAndSearchKey(input = nearest_search_df.loc[nearest_search_df.index[0], 'referrer'] , lookingFor='domain')
            search_key = getDomainAndSearchKey(input = nearest_search_df.loc[nearest_search_df.index[0], 'referrer'] , lookingFor='searchkey')
            total_revenue = revenueFromProductList(pe_product_list)
            
            revenue_by_searchkey_list.append({'Search Engine Domain':search_engine.lower() , 'Search Keyword':search_key.lower() , 'Revenue':total_revenue})
    
    searchKeyRevenue_df = pd.DataFrame(revenue_by_searchkey_list)
    
    #get total revenue for each and search engine and corresponding keyword
    output_df = searchKeyRevenue_df[searchKeyRevenue_df.Revenue>0].groupby(['Search Engine Domain', 'Search Keyword'] , as_index=False)["Revenue"].agg('sum').sort_values(by='Revenue', ascending=False)
    
    #save output to S3
    prefix = 'dev/output'
    utils_fns.writeToS3(bucket , prefix , output_df)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Revenue From Search engine - pipeline')
    }