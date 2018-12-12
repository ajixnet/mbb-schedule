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
			FilterExpression = " scheduled = :flse",
			ExpressionAttributeNames = {"#mnth":"month"},
			ExpressionAttributeValues = {":flse":{"S":"false"},":m":{"S":m}}
			)
		if (responses['Count'] != 0):
			game = responses['Items'][0]
			return game
	print("errored out; no games to schedule")		
	return "oops"
	
def mapruletofn(lambdaclient, fn_name, isTest, ruleArn, ruleName):
	client = boto3.client('events')
	############3 Try the cloudwatch put_targets api
	fnInfo = lambdaclient.get_function(FunctionName = fn_name)
	fnArn = fnInfo['Configuration']['FunctionArn']
	response = client.put_targets(
		Rule = ruleName,
		Targets = [
			{
				'Id' : fn_name,
				'Arn' : fnArn
			}
		]
	)
	
	statementid = "AllowTriggerCWEvent-" + ruleName
	print(ruleArn + "is the rule arn. (make sure it's the full ARN)")
	added_permission = lambdaclient.add_permission(
		FunctionName = fn_name,
		StatementId = statementid,
		Action = 'lambda:InvokeFunction',
		Principal = 'events.amazonaws.com',
		SourceArn = ruleArn
	)
	
	#uuid = response['UUID']
	return #uuid
	
def makeCWrule(date_s, time_s):
	client = boto3.client('events')
	month, day = date_s.split('/', maxsplit=1)
	day_alert = day
	#print("time_s is: " + time_s)
	hours, mins = time_s.split(':', maxsplit=1)
	hr_alert_eastern = int(hours)-1 #alert 1 hour before game
	hr_alert_utc = hr_alert_eastern + 5 #fixes UTC weirdness, hopefully
	if (hr_alert_utc >= 24):
		hr_alert_utc = hr_alert_utc - 24
		day_int = int(day)+1
		day_alert = str(day_int)
	hr_alert = str(hr_alert_utc)
	if (int(month) < 5): 
		year = "2019"
	else: 
		year = "2018"
	sched = "cron(" + mins + " " + 	hr_alert + " " + day_alert.zfill(2) + " " + month + " " + 	"? " + 	year + ")"
	#print("month is: " + month)
	#print("day is: " + day)
	#print("hours is: " + hours)
	#print("mins is: " + mins)
	#print("hr_alert is: " + hr_alert)
	#print("sched is: " + sched)
	ruleName = 'MBB-Schedule-'+month+day.zfill(2)
	ruleARNdict = client.put_rule(
		Name=ruleName,
		ScheduleExpression=sched,
		State= 'ENABLED'
		)
	
	return ruleARNdict['RuleArn'], ruleName
	
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
			":tru":{"S":"true"}
			}
		)
	return

def addtoDB(tablename,gameinfo,ruleArn):
	dynamo = boto3.client('dynamodb')
	response = dynamo.update_item(
		TableName = tablename,
		Key = {"month":gameinfo['month'], "day":gameinfo['day']},
		UpdateExpression = "SET ruleArn = :rulearn, sent = :flse ,scheduled = :flse",
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
		timeout_h, timeout_m = timeout.split(':')
		if (int(timeout_h) != 12):
			timeout24 = int(timeout_h) + 12
			timeout_h = str(timeout24)
		timeout = timeout_h + ":" + timeout_m
	else:
		timeout = timein
	return timeout
	
def zerodate(dayin):
	if (int(dayin) < 10):
		dayout = "0" + dayin
		return dayout
	else:
		return dayin

def lambda_handler(event, context):
	#first instantiate the things i need
	lambdafn2 = boto3.client('lambda')
	tablename = 'MBB-Schedule-date'
	isTest = "true"
	
	#check db, receive new date and time
	rawgameinfo = checkdb(tablename)
	gameinfo = cleangame(rawgameinfo)
	print("retrieved game info below")
	print(gameinfo)
	date = gameinfo['month']+'/'+gameinfo['day']
	time = gameinfo['time']
	
	#set new event mapping
	ruleArn, ruleName = makeCWrule(date,cleantime(time)) #only sends hh:mm
	addtoDB(tablename,rawgameinfo,ruleArn)
	fn2name = "MBB-ratchet2"
	mapping_uuid = mapruletofn(lambdafn2,fn2name,isTest,ruleArn,ruleName)
	
	#update db to say i scheduled notification
	setScheduled(tablename,rawgameinfo)
	
	print("I think it worked?")
	return
