import argparse
import os
import csv
import json
import getpass

from urlparse import urlparse

class EtlParameters:

	AWS_CREDENTIALS_FILE = 'Credentials/aws_credentials.csv'
	REDSHIFT_PASSFILE = "~/.awspass"
	AWS_S3_BUCKET_DEFAULT = 'glenmistro-ia-dev'

	def __init__(self):
		#
		# Initialize member vars
		# 
		self.redshiftParams = None
		self.awsParams = None
		self.awsDefaultBucket = None
		self.rmq_params = None

	#
	# Redshift Parameters
	# Figure out the default userid / password for AWS Redshift
	#
	def getRedshiftParameters(self):

		if self.redshiftParams != None:
			return self.redshiftParams

		# Initialize the hash
		self.redshiftParams = {
			'username' : None,
			'password' : None,
			'redshiftInstance' : None,
			'redshiftPort' : '5439',
			'redshiftDB' : None
			}

		passpath = os.path.expanduser(self.REDSHIFT_PASSFILE)
		if ('AWS_REDSHIFT_USER' in os.environ) and ('AWS_REDSHIFT_PWD' in os.environ):
			print "Read AWS Redshift parameters from environment vars"
			self.redshiftParams['username']         = os.environ['AWS_REDSHIFT_USER']
			self.redshiftParams['password']         = os.environ['AWS_REDSHIFT_PWD']
			self.redshiftParams['redshiftInstance'] = os.environ['AWS_REDSHIFT_INSTANCE']
			self.redshiftParams['redshiftPort']     = os.environ['AWS_REDSHIFT_PORT']
			self.redshiftParams['redshiftDB']       = os.environ['AWS_REDSHIFT_DB']
		elif os.path.exists(passpath):
			with open(passpath,'r') as f:
				line = f.readline()
				password = line.strip()
				print "Read AWS Redshift parameters from: [", self.REDSHIFT_PASSFILE, "]"
				self.redshiftParams['username']         = "awsuser"
				self.redshiftParams['password']         = password
				self.redshiftParams['redshiftInstance'] = None
				self.redshiftParams['redshiftPort']     = '5439'
				self.redshiftParams['redshiftDB']       = 'mydb'
		else:
			print "No .awspass and no AWS_REDSHIFT_USER env var. Perhaps need ../env_vars.sh?"

		print "Redshift parameters: username [", self.redshiftParams['username'], "] instance [", self.redshiftParams['redshiftInstance'], "]"
		return self.redshiftParams

	#
	# AWS API Credentials
	# Figure out the default userid / password for AWS Redshift
	#
	def getAWSParameters(self):
		
		if self.awsParams != None:
			return self.awsParams

		# Initialize the hash
		self.awsParams = {
			'awsUser' : None,
			'awsAccessKey' : None,
			'awsSecretKey' : None
		}

		# See if we got AWS credentials in env vars
		if ('AWS_ACCESS_KEY' in os.environ) and ('AWS_SECRET_KEY' in os.environ):
			self.awsParams['awsUser']     = None
			self.awsParams['awsAccessKey']= os.environ['AWS_ACCESS_KEY']
			self.awsParams['awsSecretKey']= os.environ['AWS_SECRET_KEY']
		else:
			# See if we got AWS credentials in a file
			with open(self.AWS_CREDENTIALS_FILE, 'rb') as aws_csv_file:
				msgsreader = csv.reader(aws_csv_file)
				for row in msgsreader:
					self.awsParams['awsUser']     = row[0]
					self.awsParams['awsAccessKey']= row[1]
					self.awsParams['awsSecretKey']= row[2]

		print "AWS parameters: accessKey [", self.awsParams['awsAccessKey'], "]"
		return self.awsParams

	#
	# AWS S3 bucket name
	# Figure out a dev specific bucket name if we have a dev user id
	# Don't do this if None or root
	#
	def awsDefaultBucketName(self):
		if self.awsDefaultBucket != None:
			return self.awsDefaultBucket

		# Try to read the S3 bucket out of the environment
		try:
			self.awsDefaultBucket = os.environ['AWS_S3_BUCKET']
			print 'AWS S3 bucket name: [', self.awsDefaultBucket, "]"
			return self.awsDefaultBucket
		except KeyError as e:
			pass

		# Try to construct the bucket name from a default
		self.awsDefaultBucket = self.AWS_S3_BUCKET_DEFAULT
		# If there's a user ID we use it
		try:
			devUserId = getpass.getuser()
		except Exception as e:
			print "Didn't find userid: ", str(e)
			devUserId = None
		if not (devUserId == 'root' or devUserId == None):
			self.awsDefaultBucket += '-' + devUserId

		print 'AWS S3 bucket name: [', self.awsDefaultBucket, "]"
		return self.awsDefaultBucket

	#
	# RabbitMQ settings (if relevant)
	#
	def getRabbitMQParameters(self):
		
		if self.rmq_params != None:
			return self.rmq_params

		## Check for and pull in RabbitMQ settings if Running in Cloud Foundry
		if ('VCAP_SERVICES' in os.environ):
			print "Found Cloud Foundry CloudAMQP credentials"
			vcap_services = json.loads(os.environ['VCAP_SERVICES'])
			# XXX: avoid hardcoding here
			cloudamqp_srv = vcap_services['cloudamqp'][0]
			if cloudamqp_srv:
				cloudamqp_uri = cloudamqp_srv['credentials']['uri']
				cloudamqp_cred = urlparse(cloudamqp_uri)
				self.rmq_params = {
					'useRabbit': True,
					'hostname' : cloudamqp_cred.hostname,
					'port'     : cloudamqp_cred.port if cloudamqp_cred.port else "5672", # CloudAMQP don't typically specify port number so I defaulted it. Note it's a string
					'username' : cloudamqp_cred.username,
					'password' : cloudamqp_cred.password,
					'vhost'    : cloudamqp_cred.path[1:]
					}
		elif 'MQ_PORT' in os.environ:
			print "Found Docker RMQ credentials"
			self.rmq_params = {
				'useRabbit': True,
				'hostname' : os.environ['MQ_PORT_5672_TCP_ADDR'],
				'port'     : os.environ['MQ_PORT_5672_TCP_PORT'],
				'username' : 'guest',
				'password' : 'guest',
				'vhost'    : '/'
				}
		else:
			print "Extracting default localhost rabbit credentials"
			self.rmq_params = {
				'useRabbit': False,
				'hostname' : '127.0.0.1',
				'port'     : '5672',
				'username' : 'guest',
				'password' : 'guest',
				'vhost'    : '/'
			}

		print "RMQ credentials: username [", self.rmq_params['username'], ']'
		return self.rmq_params

	# construct the argument parser
	# broken out from processArgs so that it can be extended in subclasses
	def getArgParser(self):
		argParser = argparse.ArgumentParser(description='pull metadata from IMAP and upload to Redshift via S3')
		argParser.add_argument('--format', metavar='<format>', type=str, default='csv',
			help="ouput format for messages file ('json' or 'csv').")
		argParser.add_argument('--outdir', metavar='<outdir>', type=str, default='data',
			help="directory in which to place output files")
		argParser.add_argument('--useRabbit', metavar='<useRabbit>', type=bool, default = self.getRabbitMQParameters()['useRabbit'],
			help="connect to RabbitMQ for credentials. If False request credentials locally.")
		argParser.add_argument('--rabbitIP', metavar='<rabbitIP>', type=str, default = self.getRabbitMQParameters()['hostname'],
			help="RabbitMQ IP address.")
		argParser.add_argument('--rabbitPort', metavar='<rabbitPort>', type=int, default = self.getRabbitMQParameters()['port'],
			help="RabbitMQ port number")
		argParser.add_argument('--rabbitUser', metavar='<rabbitUser>', type=str, default = self.getRabbitMQParameters()['username'],
			help="RabbitMQ user ID.")
		argParser.add_argument('--rabbitPwd', metavar='<rabbitPwd>', type=str, default = self.getRabbitMQParameters()['password'],
			help="RabbitMQ password.")
		argParser.add_argument('--rabbitVHost', metavar='<rabbitVHost>', type=str, default = self.getRabbitMQParameters()['vhost'],
			help="RabbitMQ VHost.")
		argParser.add_argument('--awsAccessKey', metavar='<awsAccessKey>', type=str, default = self.getAWSParameters()['awsAccessKey'],
			help="AWS Access Key.")
		argParser.add_argument('--awsSecretKey', metavar='<awsSecretKey>', type=str, default = self.getAWSParameters()['awsSecretKey'],
			help="AWS Secret Key.")
		argParser.add_argument('--awsS3Bucket', metavar='<awsS3Bucket>', type=str, default = self.awsDefaultBucketName(),
			help="AWS S3 Bucket name.")
		argParser.add_argument('--redshiftUser', metavar='<redshiftUser>', type=str, default = self.getRedshiftParameters()['username'],
			help="Redshift User ID.")
		argParser.add_argument('--redshiftPwd', metavar='<redshiftPwd>', type=str, default = self.getRedshiftParameters()['password'],
			help="Redshift Password.")
		argParser.add_argument('--redshiftInstance', metavar='<redshiftInstance>', type=str, default = self.getRedshiftParameters()['redshiftInstance'],
			help="Redshift instance DNS name.")
		argParser.add_argument('--redshiftPort', metavar='<redshiftPort>', type=str, default = self.getRedshiftParameters()['redshiftPort'],
			help="Redshift port #.")
		argParser.add_argument('--redshiftDB', metavar='<redshiftDB>', type=str, default = self.getRedshiftParameters()['redshiftDB'],
			help="Redshift database name.")
		argParser.add_argument('--messagesFile', metavar='<messagesFile>', type=str, default='messages',
			help="Root name of the file to hold the messages")
		argParser.add_argument('--recipientsFile', metavar='<recipientsFile>', type=str, default='recipients',
			help="Root name of the file to hold the recipients")
		argParser.add_argument('--messageIdsFile', metavar='<messageIdsFile>', type=str, default='message_ids',
			help="Root name of the file to hold the message IDs")
		argParser.add_argument('--userID', metavar='<userID>', type=int,
			help="User ID to use for local runs")
		return argParser

	#
	# Process the arguments passed to the app
	# We default a lot of these to the env vars etc collected above.
	# User can provide most parameters using env vars and override them on the command line
	#
	def processArgs(self):
		argParser = self.getArgParser()
		return argParser.parse_args()