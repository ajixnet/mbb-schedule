from __future__ import print_function
import boto3
import os
import sys
import uuid
import csv
     
s3_client = boto3.client('s3')

def csv2json(s3obj):
	csvfile = open(s3obj,'r')
	jsonfile = open(/tmp/mbb.json,'w')
	
	fieldnames = ("opponent","isHome","date","day","time")
	reader = csv.DictReader(csvfile,fieldnames)
	for row in reader:
		json.dump(row, jsonfile)
		jsonfile.write('\n')
     
def handler(event, context):
	for record in event['Records']:
		bucket = record['s3']['bucket']['name']
		key = record['s3']['object']['key'] 
		download_path = '/tmp/{}{}'.format(uuid.uuid4(), key)
		upload_path = '/tmp/resized-{}'.format(key)
	
	s3_client.download_file(bucket, key, download_path)
	s3_client.upload_file(upload_path, '{}resized'.format(bucket), key)
	
