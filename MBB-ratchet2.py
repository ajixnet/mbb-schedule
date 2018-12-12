import json
import boto3

def checkdb2(tablename,eventTime):
	#get the database entry for this game, based on the event date/time 
	# this makes the assumption that the function fired correctly - dubious at best haha
	# but it does minimize the number of reads, b/c it can just get the primary/sort key entry directly
	dynamo = boto3.client('dynamodb')
	y, m, restOfTime = eventTime.split('-', maxsplit=2)
	d = restOfTime[:2]
	responses = dynamo.query(
		TableName = tablename,
		KeyConditionExpression =  '#mnth = :m AND #dy = :d',
		ExpressionAttributeNames = {"#mnth":"month", "#dy":"day"},
		ExpressionAttributeValues = {":m":{"S":m},":d":{"S":d}}
		)
	if (responses['Count'] == 0):
		d = str(int(d)-1).zfill(2)
		responses = dynamo.query(
			TableName = tablename,
			KeyConditionExpression =  '#mnth = :m AND #dy = :d',
			ExpressionAttributeNames = {"#mnth":"month", "#dy":"day"},
			ExpressionAttributeValues = {":m":{"S":m},":d":{"S":d}}
			)
	game = responses['Items'][0]
	return game

def cleangame(rawgame):
	rawjson = rawgame
	cleanjson = {}
	for i in rawjson:
		cleanjson[i] = rawjson[i]['S']
	return cleanjson
	
def sendsms(gameinfo):
	sns = boto3.resource('sns')
	topic = sns.Topic("<put ARN here - removed for github>")
	msg = "Michigan Basketball will play @"
	if (gameinfo['isHome'] == 'true'):
		msg = msg + " home against "
	msg = msg +	gameinfo['opponent'] + " at " + gameinfo['time']
	response = topic.publish(
		Message = msg
	)
	return response
	
def triggerfn1():
	#first instantiate the things i need
	client = boto3.client('lambda')
	fn1Name = "MBB-ratchet1"
	#trigger fn1; try lambda.invoke api
	response = client.invoke(
		FunctionName = fn1Name
	)
	
	
	return response

def cleanup(tablename,gameinfo):
	#instantiate the things i need
	client = boto3.client('events')
	print("inside cleanup")
	
	#deletes previous rule
	ruleName = 'MBB-Schedule-' + gameinfo['month'] + gameinfo['day']
	print("rule name is: " + ruleName)
	
	targetsDict = client.list_targets_by_rule(
		Rule = ruleName
	)
	
	targetinfolist = targetsDict['Targets']
	print(targetinfolist)
	targetlist = []
	for target in targetinfolist:
		targetlist.append(target['Id'])
	
	print("below is parsed target list")
	print(targetlist)
	removeresponse = client.remove_targets(
		Rule = ruleName,
		Ids = targetlist
	)
	print(removeresponse)
	delete_response = client.delete_rule(
		Name = ruleName
	)
	print(delete_response)
	
	lambdaclient = boto3.client('lambda')
	statementid = "AllowTriggerCWEvent-" + ruleName
	rem_perm_resp = lambdaclient.remove_permission(
		FunctionName = targetlist[0], #assumes 1 target in list, which is expected
		StatementId = statementid
	)
	print(rem_perm_resp)
	print("leaving cleanup")
	return

def setSent(tablename,gameinfo):
	print("inside setsent")
	dynamo = boto3.client('dynamodb')
	response = dynamo.update_item(
		TableName = tablename,
		Key = {"month":gameinfo['month'], "day":gameinfo['day']},
		UpdateExpression = "SET sent = :tru",
		ExpressionAttributeValues = {
			":tru":{"S":"true"}
			}
		)
	print("This is the key i'm sending: " + str({"month":gameinfo['month'], "day":gameinfo['day']}))
	print("leaving setsent")
	return
	

def lambda_handler(event, context):
	#first instantiate the things i need
	tablename = 'MBB-Schedule-date'
	
	#check db, receive new date and time
	rawgameinfo = checkdb2(tablename,event['time'])
	gameinfo = cleangame(rawgameinfo)
	print("received game info below")
	print(gameinfo)
	#sms_response = "test mode only"
	sms_response = sendsms(gameinfo)
	print('Message sent. ' + str(sms_response))
	
	#adds the flag to the db
	setSent(tablename,rawgameinfo)
	cleanup(tablename,gameinfo)
	
	print("after cleanup gameinfo")
	print(cleangame(checkdb2(tablename,event['time'])))
	print("firing fn1?")
	#trigger re-scheduling lambda
	fired1 = triggerfn1()
	print(str(fired1))
	
	return "fn2 success?"
