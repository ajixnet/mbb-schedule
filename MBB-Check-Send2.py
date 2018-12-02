import json


def sendsms():
	return
	
def triggerfn1():
	#first instantiate the things i need
	
	#trigger fn1
	
	#return 1 for good or 0 for failed
	
	return

def cleanup():
	#instantiate the things i need
	
	#adds "sent sms" entry to current game
	
	return


def lambda_handler(event, context):
	#first instantiate the things i need
	
	
	#check db, receive new date and time
	sendsms()
	
	#adds the flag to the db
	cleanup()
	
	#trigger re-scheduling lambda
	triggerfn1()
	
	return {
		'statusCode': 200,
		'body': json.dumps('Hello from Lambda!')
	}
