from __future__ import print_function
import boto3
import json
import os
import sys
import uuid
import csv
     
s3_client = boto3.client('s3')

def csv2json(s3obj):
	csvfile = open(s3obj,'r')
	jsonfile = open('/tmp/mbb.json','a')
	
	fieldnames = ("opponent","isHome","month","day","dow","time","scheduled")
	reader = csv.DictReader(csvfile,fieldnames)
	
	print("inside the csv2json function")
	#print(csvfile.read())
	#print(csvfile,jsonfile,fieldnames,reader)
	#print("going into loop")
	for row in reader:
		jsonfile.write(json.dumps(row))
		jsonfile.write('\n')
		#print(json.dumps(row))
	jsonfile.close()
	return '/tmp/mbb.json', jsonfile
    
def upjson2dynamo(filepath, tablename):
	print("inside the upload function")
	#tablename = "MBB-Schedule-date"
	jsonfile = open(filepath,'r')
		
	dynamodb = boto3.resource('dynamodb')
	table = dynamodb.Table(tablename)
	
	with table.batch_writer() as batch:
		for i in jsonfile.readlines():
			batch.put_item(Item=json.loads(i))
			#print("Uploaded game: ", str(i), " to table")
	
	#for i in jsonfile.readlines():
	#	table.put_item(Item=json.loads(i))
	#	print("Uploaded game: ", str(i), " to table")



def lambda_handler(event, context):
	for record in event['Records']:
		bucket = record['s3']['bucket']['name']
		key = record['s3']['object']['key'] 
		download_path = '/tmp/{}{}'.format(uuid.uuid4(), key)
		#upload_path = '/tmp/testjson-{}'.format(key)
	
	s3_client.download_file(bucket, key, download_path)
	upload_path, jsonfile = csv2json(download_path)
	#s3_client.upload_file('/tmp/mbb.json', bucket, 'mbb.json')
	#print('Function finished - 1')
	#jsonfile2 = open('/tmp/mbb.json', 'r')
	#with open('/tmp/mbb.json', 'r') as myfile:
	#	data=myfile.read()
	#print(data)
	#print("After read")
	#print(upload_path,jsonfile,jsonfile2)
	upjson2dynamo(upload_path,"MBB-Schedule-date")	
	#print("Function finished - 2")
	return
	
