#!/usr/bin/env python3
import boto3, json
from time import sleep

def create_athena_DB(database, regionName, DBbucket):
    #Athena database and table definition
    create_database = "CREATE DATABASE IF NOT EXISTS %s;" % (database)
    
    client = boto3.client('athena', region_name = regionName)
    config = {'OutputLocation': DBbucket}
    res = client.start_query_execution(
                             QueryString = create_database, 
                             ResultConfiguration = config)
    return res



#Function for executing athena queries
def run_query(query, database, s3_output):
    client = boto3.client('athena', region_name = 'ap-southeast-2') # this region is a must, don't work otherwise
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
            },
        ResultConfiguration={
            'OutputLocation': s3_output,
            }
        )
    print('Execution ID: ' + response['QueryExecutionId'])
    return response
     







# function to get the query results in a json format
# Be default, Athena stores the result as a csv file on s3 bucket
def getResults(q, database, s3_ouput, s3bucketOutputPrefix, DBbucket, regionName):
    
    print("Executing query: %s" % (q))
    res = run_query(q, database, s3_ouput)
    
    client = boto3.client('athena', region_name = regionName)

    
    # check the query status, keep looping unless there is a success
    # break out in case there is a failure in query
    queryStatus = "RUNNING"
    while (queryStatus  != "SUCCEEDED"):
        try:
            sleep(1)
            queryStatus = client.get_query_execution(QueryExecutionId = res['QueryExecutionId'])['QueryExecution']['Status']['State']
            if(queryStatus == "FAILED"):
                break
        except:
            pass
                
        
    # if query succedded, grab the result        
        
    
    try:
        r = client.get_query_results(QueryExecutionId = res['QueryExecutionId'])
        j =  json.dumps(r['ResultSet']['Rows'])

    except:
        j = json.dumps([{"results" : "An error occurred. Please check your parameters, REGION and S3 resources on AWS."}])
        
    return j
                


