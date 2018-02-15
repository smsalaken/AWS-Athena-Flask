#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 15 10:18:41 2018

@author: syed.moshfeq.salaken

run with: 
$ chmod a+x app.py
$ ./app.py


install flask
configure AWS credential with AWS CLI: run >> AWS CONFIGURE
boto3 uses that
explicitly state region
supplied S3 bucket must follow the following convention:
    <bucket name>/input : this will contain the json/csv file containing data
    <bucket name>/results : this will contain the output results
    
take care of serde in CREATE TABLE STATEMENT, need to think how we can remove hardcoding on this


"""

#!flask/bin/python
from flask import Flask, jsonify, make_response, abort
from helpers_s3 import create_athena_DB, run_query, getResults # import everything for fetching data from s3
from time import sleep


app = Flask(__name__)

@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)

tasks = [
    {
        'id': 1,
        'title': u'Buy groceries',
        'description': u'Milk, Cheese, Pizza, Fruit, Tylenol', 
        'done': False
    },
    {
        'id': 2,
        'title': u'Learn Python',
        'description': u'Need to find a good Python tutorial on the web', 
        'done': False
    }
]

# format of API:
# http://localhost:5000/s3/athena/servain-isf-1-s3/ap-southeast-2/test_database/persons/select * from test_database.persons where age > 25


@app.route('/s3/athena/<string:bucketname>/<string:AWSregion>/<string:database_name>/<string:table_name>/<string:DTquery>', methods=['GET'])
def get_task(bucketname, database_name, table_name, DTquery, AWSregion):
    #task = [task for task in tasks if task['id'] == task_id]
    task = [
            {
             "bucket_name" : bucketname,
             "database_name" : database_name,
             "table_name" : table_name,
             "DTquery" : DTquery,
             "AWSregion" : AWSregion
             
             }
            ]
    if len(task) == 0:
        abort(404)
        
    # user input
    #Athena configuration
    s3_input = 's3://' + bucketname + '/input' #'s3://servain-isf-1-s3/input'
    s3_ouput = 's3://' + bucketname + '/results/'#'s3://servain-isf-1-s3/results/'
    database = database_name #'test_database'
    table = table_name #'persons'
    regionName = AWSregion #'ap-southeast-2'
    DBbucket = 's3://' + bucketname + '/' #'s3://servain-isf-1-s3/'
    s3bucketOutputPrefix = 'results/'
    
#    print(s3_input)
#    print(s3_ouput)
#    print(database)
#    print(table)
#    print(regionName)
#    print(DBbucket)
#    print(s3bucketOutputPrefix)
#    print(DTquery)
#    
    
    
    
    # >> we need to think how to take care of this table creation sql from user <<
    
    # create table STATEMENT from json file
    # for CSV files, use wither LazySimpleSerde or OpenCSVSerde
    # see: https://docs.aws.amazon.com/athena/latest/ug/csv.html
    create_table = \
        """CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (
        `name` string,
        `sex`string,
        `city` string,
        `country` string,
        `age` int,
        `job` string
         )
         ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
         WITH SERDEPROPERTIES (
         'serialization.format' = '1'
         ) LOCATION '%s'
         TBLPROPERTIES ('has_encrypted_data'='false');""" % ( database, table, s3_input )
         
    # fetching data: Query definitions
    #query_1 = "SELECT * FROM %s.%s where sex = 'F';" % (database, table)
    #query_2 = "SELECT * FROM %s.%s where age > 30;" % (database, table)
    
    # actions:
    # create the database
    res_db = create_athena_DB(database, regionName, DBbucket)
    sleep(0.5) # necessary, otherwise, check for query status
    
    # create table on the db
    res = run_query(create_table, database, s3_ouput)
    sleep(0.5) # necessary
    
    # run query 1
    result = getResults(DTquery, database, s3_ouput, s3bucketOutputPrefix, DBbucket, regionName)
        
    #return  jsonify({'task': task[0]})
    return result

if __name__ == '__main__':
    app.run(debug=True)