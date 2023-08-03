## credentials were not present so we added dummy credentials - botocore.exceptions.NoCredentialsError: Unable to locate credentials
## region added to the env - botocore.exceptions.NoRegionError: You must specify a region.
## integer column in ddl for app version
## since all columns are nullable if one of the fields is null we will insert into postgres
## if body is not present in the message
## wait time can be configured in case of optimization

import json
import boto3
import base64
import psycopg2
import logging
from typing import Dict
from botocore.exceptions import ClientError
from configparser import ConfigParser
import hashlib
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__) 

class SQSToPostgres:

	def __init__(self, config_file : str):
		
		sqs_credentials = self.getSQSCredentials(config_file)
		self.region_name = sqs_credentials['region_name']
		self.endpoint = sqs_credentials['endpoint']
		# logger.exception("Endpoint: %s", self.endpoint)
		self.queue_name = sqs_credentials['queue_name']
		self.aws_access_key_id = sqs_credentials['aws_access_key_id']
		self.aws_secret_access_key = sqs_credentials['aws_secret_access_key']

		database_credentials = self.getPostgresCredentials(config_file)
		self.database = database_credentials['database']  
		self.user = database_credentials['user']
		self.password = database_credentials['password']
		self.host = database_credentials['host']
		self.port = database_credentials['port']

		# raise error

	def getSQSCredentials(self, config_file : str):

		#Read config.ini file
		config_object = ConfigParser()
		config_object.read(config_file)

		sqs_credentials = dict(config_object["SQS"])
		
		return sqs_credentials

	def getPostgresCredentials(self, config_file : str):

		#Read config.ini file
		config_object = ConfigParser()
		config_object.read(config_file)

		database_credentials = dict(config_object["postgres"])
		
		return database_credentials

	def encode(self, data : str) -> str:
		'''
			Encode the data in base64 format string

			Parameters:
				data: data has to be strictly in string format

			Returns:
				Returns the data in string format by first encoding in ascii bytes and then encoding in base64 format. 
				Finally, decoding using the ascii format to get base64 encoded string
		'''

		## if the data is null, then it cannot be encoded
		if data is None:
			return data
		
		hash = hashlib.sha512(str(data).encode("utf-8")).hexdigest()

		return hash

	def fetchDataFromSQS(self) -> Dict[str, str]:
		'''
			Fetch data from SQS, process one message, flatten JSON to obtain the columns that are present in the Postgres database and finally, delete message from the queue
			
			Parameters:
				region_name : Region name of the AWS server
				endpoint : Endpoint of SQS
				queue_name : Name of SQS queue
				aws_access_key_id : AWS access key Id
				aws_secret_access_key : AWS secret access key ID

			Returns:
				Dictionary with values for user_id, app_version, device_type, masked_ip, locale, masked_device_id, create_date for the first message in the queue
		'''

		try:
			# Create an SQS client
			sqs = boto3.client('sqs', self.region_name, endpoint_url = self.endpoint, aws_access_key_id = self.aws_access_key_id, aws_secret_access_key = self.aws_secret_access_key)

			wait_time = 20
			
			# Receive messages from the queue
			payload = sqs.receive_message(
							QueueUrl = self.endpoint + '/' + self.queue_name,
							MaxNumberOfMessages = 1,
							WaitTimeSeconds = wait_time
					   )

			if 'Messages' in payload and len(payload['Messages']) > 0:
				try:
					for record in payload['Messages']:
						postgres_data_dict = {}
						postgres_data_dict['MessageId'] = record['MessageId']
						
						if 'Body' in record.keys():
							body = json.loads(record['Body'])
							for field in body:
								if field == 'ip':
									postgres_data_dict['masked_ip'] = self.encode(body[field])
								elif field == 'device_id':
									postgres_data_dict['masked_device_id'] = self.encode(body[field])
								elif field == 'app_version':
									postgres_data_dict[field] = int(body[field].split('.')[0])
								else:
									postgres_data_dict[field] = body[field]
							
							postgres_data_dict['create_date'] = datetime.now().strftime("%Y-%m-%d")

						else:
							logger.info("Body not present in message with ID: %s", record['MessageId'])
							
						receipt_handle = record['ReceiptHandle']

						# Delete received message from queue
						sqs.delete_message(
											QueueUrl = self.endpoint + '/' + self.queue_name,
											ReceiptHandle = receipt_handle
										)

						# print('Received and deleted message: %s' % msg['MessageId'])
						logger.info("Received message with MessageId: %s and ReceiptHandle: %s", record['MessageId'], record['ReceiptHandle'])

						yield postgres_data_dict

				except KeyError as e:
					logger.exception("The key Messages is not present in the message %s", payload)				

		except ClientError as error:
			logger.exception("Couldn't receive messages from queue: %s", self.endpoint + '/' + self.queue_name)
			raise error


	def writeToPostgres(self, data : Dict[str, str]) -> None:
		'''
			Function to write the messages from SQS to Postgres

			Parameters:
				database_credentials : Database credentials to connect to the postgres database. Keys of the dictionary - database, user, password, host and port

			Returns:
				None
		'''

		# database = database_credentials['database']  
		# user = database_credentials['user']
		# password = database_credentials['password']
		# host = database_credentials['host']
		# port = database_credentials['port']

		message_id = data['MessageId']
		user_id = data['user_id'] if 'user_id' in data else None
		# if version has another alphanumeric character it can break the code
		app_version = data['app_version'] if 'app_version' in data else None  # if it doesn't have dot automatically will select the first digit
		device_type = data['device_type'] if 'device_type' in data else None 
		masked_ip = data['masked_ip'] if 'masked_ip' in data else None 
		locale = data['locale'] if 'locale' in data else None 
		masked_device_id = data['masked_device_id'] if 'masked_device_id' in data else None 
		create_date = data['create_date'] if 'create_date' in data else None 
		
		# sql = f'''INSERT INTO user_logins(user_id, device_type, masked_ip, masked_device_id, locale, app_version, create_date) 
		# 		  VALUES('{user_id}', '{device_type}', '{masked_ip}', '{masked_device_id}', 
		# 		  		 '{locale}', {app_version}, {create_date});'''

		sql = '''INSERT INTO user_logins(user_id, device_type, masked_ip, masked_device_id, locale, app_version, create_date) 
			      VALUES(%s, %s, %s, %s, %s, %s, %s);'''

		# print(sql)

		#establishing the connection
		conn = psycopg2.connect(
				   database=self.database, 
				   user=self.user, 
				   password=self.password, 
				   host=self.host, 
				   port=self.port
			   )

		#Creating a cursor object using the cursor() method
		cursor = conn.cursor()

		try:
			# Executing an MYSQL function using the execute() method
			cursor.execute(sql, (user_id, device_type, masked_ip, masked_device_id, locale, app_version, create_date))

			conn.commit()

			#Closing the connection
			conn.close()

		# cursor.execute(sql)
		# 	print(sql, (user_id, device_type, masked_ip, masked_device_id, locale, app_version, create_date))
		# 	print(cursor._executed)

		except:
			logger.exception("Issue while writing the message %s", message_id)