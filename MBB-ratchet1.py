import json
import boto3


def checkdb(tablename):
	#get first game where no notification was sent
	dynamo = boto3.client('dynamodb')
	months = ['11','12','01','02','03','04']
	days = range(1,32)
	for m in months:
		responses = dynamo.query(
			TableName = tablename,
			KeyConditionExpression =  '#mnth = :m',
			FilterExpression = " ruleArn <> :nll",
			ExpressionAttributeNames = {"#mnth":"month"},
			ExpressionAttributeValues = {":nll":{"NULL":bool('true')},":m":{"S":m}}
			)
		if (responses['Count'] != 0):
			game = responses['Items'][1]
			return game
	print("errored out; no games to schedule")		
	return "oops"
	
def mapruletofn(lambdafunc, fn_name, isTest, ruleArn):
	response = lambdafunc.create_event_source_mapping(
		EventSourceArn=ruleArn,
		FunctionName=fn_name
		)
	uuid = response['UUID']
	return uuid
	
def makeCWrule(date_s, time_s):
	client = boto3.client('events')
	month, day = date_s.split('/', maxsplit=1)
	print("time_s is: " + time_s)
	hours, mins = time_s.split(':', maxsplit=1)
	hr_alert = str(int(hours)-1)
	if (int(month) < 5): 
		year = "2019"
	else: 
		year = "2018"
	sched = "cron(" + mins + " " + 	hr_alert + " " + day + " " + month + " " + 	"? " + 	year + ")"
	print("month is: " + month)
	print("day is: " + day)
	print("hours is: " + hours)
	print("mins is: " + mins)
	print("hr_alert is: " + hr_alert)
	print("sched is: " + sched)
	ruleARNdict = client.put_rule(
		Name='MBB-Schedule-'+month+day,
		ScheduleExpression=sched,
		State= 'ENABLED'
		)
	
	return ruleARNdict['RuleArn']
	
def cleangame(rawgame):
	rawjson = rawgame
	cleanjson = {}
	for i in rawjson:
		cleanjson[i] = rawjson[i]['S']
	#cleangame = json.dumps(cleanjson)
	return cleanjson #previously "cleangame"
	
def setScheduled(tablename,gameinfo):
	dynamo = boto3.client('dynamodb')
	response = dynamo.update_item(
		TableName = tablename,
		Key = {"month":gameinfo['month'], "day":gameinfo['day']},
		UpdateExpression = "SET scheduled = :tru",
		ExpressionAttributeValues = {
			"tru":{"S":"true"}
			}
		)
	return

def addtoDB(tablename,gameinfo,ruleArn):
	dynamo = boto3.client('dynamodb')
	response = dynamo.update_item(
		TableName = tablename,
		Key = {"month":gameinfo['month'], "day":gameinfo['day']},
		UpdateExpression = "SET ruleArn = :rulearn, sent = :flse, scheduled = :flse",
		ExpressionAttributeValues = {
			":rulearn":{"S":ruleArn},
			":flse":{"S":"false"}
			}
		)
	return response

def cleantime(timein):
	if timein.endswith('am'):
		timeout = timein.rstrip('am')
	elif timein.endswith('pm'):
		timeout = timein.rstrip('pm')
	else:
		timeout = timein
	return timeout
	
def lambda_handler(event, context):
	#first instantiate the things i need
	lambdafn2 = boto3.client('lambda')
	tablename = 'MBB-Schedule-date'
	isTest = "true"
	
	#check db, receive new date and time
	rawgameinfo = checkdb(tablename)
	gameinfo = cleangame(rawgameinfo)
	date = gameinfo['month']+'/'+gameinfo['day']
	time = gameinfo['time']
	
	#set new event mapping
	ruleArn = makeCWrule(date,cleantime(time)) #only sends hh:mm
	addtoDB(tablename,rawgameinfo,ruleArn)
	fn2name = "MBB-ratchet2"
	mapping_uuid = mapruletofn(lambdafn2,fn2name,isTest,ruleArn)
	
	#update db to say i scheduled notification
	setScheduled(tablename,rawgameinfo)
	
	print("I think it worked?")
	return