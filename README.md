## AWS Athena integration with Flask

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


boto3 and Athena tutorial: https://medium.com/@devopsglobaleli/introduction-17b4d0c592b6
