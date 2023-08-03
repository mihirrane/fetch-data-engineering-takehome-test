import json
import boto3
import psycopg2
import logging
from typing import Dict
from botocore.exceptions import ClientError
from configparser import ConfigParser
import hashlib
from datetime import datetime
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__) 

class SQSToPostgres:

	def __init__(self, config_file : str) -> None:
		'''
			Constructor method to initialize the necessary fields to connect to SQS and Postgres

			Parameters:
				config_file: Path to the config file
		'''
		
		sqs_credentials = self.getSQSCredentials(config_file)
		self.region_name = sqs_credentials['region_name']
		self.endpoint = sqs_credentials['endpoint']

		self.queue_name = sqs_credentials['queue_name']
		self.aws_access_key_id = sqs_credentials['aws_access_key_id']
		self.aws_secret_access_key = sqs_credentials['aws_secret_access_key']

		database_credentials = self.getPostgresCredentials(config_file)
		self.database = database_credentials['database']  
		self.user = database_credentials['user']
		self.password = database_credentials['password']
		self.host = database_credentials['host']
		self.port = database_credentials['port']


	def getSQSCredentials(self, config_file_name : str) -> Dict[str, str]:
		'''
			Extract credentials to connect to the AWS SQS queue using the config file

			Parameters:
				config_file: Path to the config file

			Returns:
				Dictionary of all fields listed under the 'SQS' section in config file
		'''

		#Read config.ini file
		config_object = ConfigParser()
		config_file = os.path.join(os.getcwd(), config_file_name)
		config_object.read(config_file)

		sqs_credentials = dict(config_object["SQS"])
		
		return sqs_credentials

	def getPostgresCredentials(self, config_file_name : str) -> Dict[str, str]:
		'''
			Extract credentials to connect to the postgres database using the config file

			Parameters:
				config_file: Path to the config file

			Returns:
				Dictionary of all fields listed under the 'postgres' section in config file
		'''


		#Read config.ini file
		config_object = ConfigParser()
		config_file = os.path.join(os.getcwd(), config_file_name)
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
			Fetch data from SQS, process one message, flatten JSON to obtain the columns that are present in the Postgres database and finally, delete message from the queue.
			This function is used as a generator which will generate one message at a time

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
			sqs = boto3.client('sqs', self.region_name, endpoint_url = self.endpoint, aws_access_key_id = self.aws_access_key_id,
								aws_secret_access_key = self.aws_secret_access_key)

			wait_time = 20
			
			# Receive messages from the queue
			payload = sqs.receive_message(
							QueueUrl = self.endpoint + '/' + self.queue_name,
							MaxNumberOfMessages = 1,
							WaitTimeSeconds = wait_time
					   )

			# If there is no "Messages" field in the JSON or there is an empty list under 'Messages', we cannot process the message
			if 'Messages' in payload and len(payload['Messages']) > 0:
				try:
					# For every element in the array inside the 'Messages' key of JSON, we extract the Message Id
					for record in payload['Messages']:
						postgres_data_dict = {}
						postgres_data_dict['MessageId'] = record['MessageId']
						
						# The fields to be inserted into Postgres are obtained from the 'Body' inside the list of 'Messages'
						if 'Body' in record.keys():
							body = json.loads(record['Body'])
							for field in body:
								if field == 'ip': ## PII field
									postgres_data_dict['masked_ip'] = self.encode(body[field])
								elif field == 'device_id': ## PII field
									postgres_data_dict['masked_device_id'] = self.encode(body[field])
								elif field == 'app_version': ## field which requires integer value so we extract the number until first dot
									try:
										postgres_data_dict[field] = int(body[field].split('.')[0])
									except:
										logger.warning("App version is alphanumeric or does not follow dot notation format for message with ID: %s", record['MessageId'])
								else: ## if none of the cases then plain assignment of the field
									postgres_data_dict[field] = body[field]
							
							## Create_date will be today's date in YYYY-MM-DD format
							postgres_data_dict['create_date'] = datetime.now().strftime("%Y-%m-%d")

						else:
							logger.info("Body not present in message with ID: %s", record['MessageId'])
							
						## Delete the message from the queue using the receipt handle
						receipt_handle = record['ReceiptHandle']

						# Delete received message from queue
						sqs.delete_message(
											QueueUrl = self.endpoint + '/' + self.queue_name,
											ReceiptHandle = receipt_handle
										)

						# Log with show the message id and receipt handle for every message processed by the queue
						logger.info("Received message with MessageId: %s and ReceiptHandle: %s", record['MessageId'], record['ReceiptHandle'])

						## Yields a dictionary which contains all columns and their corresponding value
						yield postgres_data_dict

				except KeyError as e:
					# If there is any error that cannot be logged by the warnings or info above, it will be logged as exception and shown below
					logger.exception("The key Messages is not present in the message %s", payload)				

		except ClientError as error:
			# In case of incorrect queue name or endpoint
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
		
		# Access the messsage id and other column values to be inserted into the postgres table from the config file dictionary
		message_id = data['MessageId']
		user_id = data['user_id'] if 'user_id' in data else None 
		app_version = data['app_version'] if 'app_version' in data else None  # if version has another alphanumeric character it can break the code # if it doesn't have dot automatically will select the first digit
		device_type = data['device_type'] if 'device_type' in data else None 
		masked_ip = data['masked_ip'] if 'masked_ip' in data else None 
		locale = data['locale'] if 'locale' in data else None 
		masked_device_id = data['masked_device_id'] if 'masked_device_id' in data else None 
		create_date = data['create_date'] if 'create_date' in data else None 

		# SQL query to insert those values, null in case the column value is not present 
		sql = '''INSERT INTO user_logins(user_id, device_type, masked_ip, masked_device_id, locale, app_version, create_date) 
			      VALUES(%s, %s, %s, %s, %s, %s, %s);'''

		# Establishing the connection
		conn = psycopg2.connect(
				   database=self.database, 
				   user=self.user, 
				   password=self.password, 
				   host=self.host, 
				   port=self.port
			   )

		# Creating a cursor object using the cursor() method
		cursor = conn.cursor()

		try:
			# Executing the query using the execute() method
			cursor.execute(sql, (user_id, device_type, masked_ip, masked_device_id, locale, app_version, create_date))

			# Commit the results into the postgres database
			conn.commit()

			# Closing the connection
			conn.close()

			return True ## Insert success

		except:
			logger.exception("Issue while writing the message %s", message_id)

			return False ## Insert failed