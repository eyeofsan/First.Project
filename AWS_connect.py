#!/usr/bin/env python
# coding: utf-8

# In[1]: Imported necessary packages


import boto3
import pandas as pd
import psycopg2
import json
import configparser


# In[2]: saving critical files from config


config= configparser.ConfigParser()
config.read_file(open('san_aws.config'))


KEY= config.get("AWS","KEY")
SECRET= config.get("AWS","SECRET")
DWH_CLUSTER_TYPE= config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES= config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE= config.get("DWH","DWH_NODE_TYPE")
DWH_CLUSTER_IDENTIFIER= config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB= config.get("DWH","DWH_DB")
DWH_DB_USER= config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD= config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT= config.get("DWH","DWH_PORT")
DWH_IAM_ROLE_NAME= config.get("DWH","DWH_IAM_ROLE_NAME")



# In[3]: creating a data frame using pandas


df = pd.DataFrame({"param":["KEY","SECRET","DWH_CLUSTER_TYPE","DWH_NUM_NODES","DWH_NODE_TYPE","DWH_CLUSTER_IDENTIFIER","DWH_DB","DWH_DB_USER","DWH_DB_PASSWORD","DWH_PORT","DWH_IAM_ROLE_NAME"],
             "value":[KEY,SECRET,DWH_CLUSTER_TYPE,DWH_NUM_NODES,DWH_NODE_TYPE,DWH_CLUSTER_IDENTIFIER,DWH_DB,DWH_DB_USER,DWH_DB_PASSWORD,DWH_PORT,DWH_IAM_ROLE_NAME]})
df


# In[35]: creating resources and clients for AWS services


s3 = boto3.resource('s3',region_name = "ap-south-1", aws_access_key_id = KEY, aws_secret_access_key = SECRET)
iam= boto3.client('iam',region_name = "ap-south-1", aws_access_key_id = KEY, aws_secret_access_key = SECRET)
redshift=  boto3.client('redshift',region_name = "ap-south-1", aws_access_key_id = KEY, aws_secret_access_key = SECRET)
ec2 = boto3.resource('ec2',region_name = "ap-south-1", aws_access_key_id = KEY, aws_secret_access_key = SECRET)


# In[10]:S3 object resource


bucket=s3.Bucket("san-test-buckets")
log_file = [filename.key for filename in bucket.objects.filter(Prefix='')]
log_file


# In[48]: saving role _arn


rolearn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']


# In[20]: getting cluster credentials; cannot be retrieved if cluster isnt active


rds_cluster = redshift.get_cluster_credentials(DbUser=DWH_DB_USER,
    DbName=DWH_DB,
    ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)
rds_cluster


# In[43]: getting only required values from the detailed JSON


def prettyrdsprop(props):
	pd.set_option('display.max_colwidth',0)
	keysToShow=["ClusterIdntifier","NodeType","ClusterStatus","MasterUsername","DBName","Endpoint","VpcId"]
	x =[(k,v) for k,v in props.items() if k in keysToShow]
	return pd.DataFrame(data = x, columns=["Key","Value"])

redshift_prop = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

prettyrdsprop(redshift_prop)


# In[31]:


DWH_ENDPOINT = redshift_prop['Endpoint']['Address']
DWH_ROLE_ARN = redshift_prop['IamRoles'][0]['IamRoleArn']
DB_NAME = redshift_prop['DBName']
DB_USER = redshift_prop['MasterUsername']


# In[38]: EC2 instance 


try:
	vpc = ec2.Vpc(id=redshift_prop['VpcId'])
	defaultSg = list(vpc.security_groups.all())[0]
	print(defaultSg)
	
	defaultSg.authorize_ingress(
		GroupName = defaultSg.group_name,
		CidrIp = '0.0.0.0/0',
		IpProtocol = 'TCP',
		FromPort = int(DWH_PORT),
		ToPort= int(DWH_PORT)			
	)
except Exception as e:
	print("Exception: ",e)


# In[44]: creating connection object


try:
	conn = psycopg2.connect(host=DWH_ENDPOINT, dbname = DB_NAME, user=DB_USER, password= DWH_DB_PASSWORD, port=DWH_PORT)
except Exception as e:
	print("Error is: ", e)
	
conn.set_session(autocommit=True)


# In[45]: ceating cursor


try:
    cur = conn.cursor()
except psycopg2.Error   as e:
    print("cursor eror is: ", e)


# In[46]: using cursor to execute a DDL statement


try:
    cur.execute("""
    create table py_table( sap_code integer, name varchar,band varchar, isctive boolean);
    """)
except psycopg2.Error as e:
    print("Error creating table: ", e)


# In[54]: data file is present in S3, using copy cmd to fetch data from s3 into redshift


try:
    cur.execute("""
    copy py_table from 's3://san-test-buckets/py_table.txt'
    credentials 'aws_iam_role=arn:aws:iam::026259293642:role/Redshif-s3-access'
    delimiter '|'
    region 'ap-south-1';
    """)
except psycopg2.Error as e:
    print("Error creating table: ", e)


# In[61]: displaying first row from python


try:
    cur.execute("""
    select * from py_table;
    """)
except psycopg2.Error as e:
    print("Error creating table: ", e)
    
row = cur.fetchone()
while row:
    print(row)
    row = cur.fetchone()
    break


# In[60]:closing connection


try:
   conn.close()
except psycopg2.Error as e:
    print("Error closing connection: ", e)

exit 0



