# - RevenueFromSearchEngine.py
import json
import boto3
import pandas as pd
import re
from io import StringIO

# Pending - 
# maintains classes
# logger modules
# document desction

#Custom exception
class MyCustomError(Exception):
    def __init__(self, *args):
        if args:
            self.message = args[0]
        else:
            self.message = None
    
    def __str__(self):
        if self.message:
            return 'MyCustomError - , {0} '.format(self.message)
        else:
            return 'MyCustomError has been raised'

def get_s3filename_from_event(event):
    if 'Records' not in event:
            return ''
    for r in event.get('Records'):
        # track only objected created events
        if not ( ( r.get('eventName') == "ObjectCreated:Put" )  and ( 's3' in r ) ) : continue
        bucket  = r['s3']['bucket']['name']
        key =  r['s3']['object']['key']
        return bucket, key

def readFilesFromS3(bucket , key):
    s3 = boto3.client('s3')
    s3_response = s3.get_object(Bucket=bucket, Key=key)
    print("Enter - readFilesFromS3")
    return s3_response

def writeToS3(bucket , prefix , output_df):
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
    

def readInputdatatoPandasDataframe(s3_response):
    print("Enter - readInputdatatoPandasDataframe")
    status = s3_response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
        input_df = pd.read_csv(s3_response.get("Body"), sep='\t')
        return input_df
    else:
        raise MyCustomError('Issue in reading S3 files')
    
def getDomainAndSearchKey(input, lookingFor='domain'):
    m = re.search('(https|http)?://([A-Za-z_0-9.-]+)((\/search)?.*(\?|&)(q=|p=)([a-zA-Z\+0-9]+).*)*', input)
    if m:
        if lookingFor=='domain':
            return m.group(2)
        elif lookingFor=='searchkey':
            searchkey = m.group(7)
            return searchkey.replace("+"," ")
    else:
        raise MyCustomError('Issue in Regex to get domain or searchkey')

def revenueFromProductList(product_list):
    revenue = 0
    for each_product in product_list.split(","):
        revenue = revenue + int(each_product.split(";")[3])
    return revenue
    

def main(event, context):
    # TODO implement
    #remove unnecessary columns
    bucket, s3_filename = get_s3filename_from_event(event)
    print("****s3_filename **** - "+s3_filename)
    
    if s3_filename =='':
        return {
        'statusCode': 501,
        'body': json.dumps('No S3 Create event is happend')
    }
    else:
        s3_response = readFilesFromS3(bucket , s3_filename)
        hitdata_df = readInputdatatoPandasDataframe(s3_response)

    
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
    writeToS3(bucket , prefix , output_df)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Revenue From Search engine - pipeline')
    }