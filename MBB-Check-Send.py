import json


def checkdb():
	#get first game where no notification was sent
	
	return

def maketime(datestring,timestring):
	#change from strings into proper crontab string somehow
	
	return
	
def triggerfn2():
	#first instantiate the things i need
	
	#update event mapping for fn2
	
	#check new mapping? return 1 for good or 0 for failed
	
	return

def cleanup():
	#update game with scheduled notification entry
	
	return


def lambda_handler(event, context):
	#first instantiate the things i need
	
	
	#check db, receive new date and time
	checkdb()
	
	#get right time format
	maketime()
	
	#set new event mapping
	triggerfn2()
	
	#update db to say i scheduled notification
	cleanup()
	
	return {
		'statusCode': 200,
		'body': json.dumps('Hello from Lambda!')
	}
