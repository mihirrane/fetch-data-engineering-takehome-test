import unittest

from utils.sqs_to_postgress import SQSToPostgres

# The test based on unittest module
class TestSQSRead(unittest.TestCase):
    def setUp(self):
        self.sqs_to_postgres = SQSToPostgres('config.ini')

    def testSQSCredentials(self):
        '''
            Function to test when the config file is read, we have all the credential fields in the dictionary to read the SQS queue
        '''

        sqs_credentials_dict = self.sqs_to_postgres.getSQSCredentials('config.ini')
        self.assertEqual('region_name' in sqs_credentials_dict, True, "Region not present")
        self.assertEqual('endpoint' in sqs_credentials_dict, True, "Endpoint not present")
        self.assertEqual('queue_name' in  sqs_credentials_dict, True, "Queue name not present")
        self.assertEqual('aws_access_key_id' in sqs_credentials_dict, True, "AWS access key id not present")
        self.assertEqual('aws_secret_access_key' in sqs_credentials_dict, True, "AWS secret access key not present")

    def testSQSRead(self):
        '''
            Function to test when the SQS queue messages are read the fields returned in the dictionary are correct and all present for inserting into postgres database
        '''
        
        postgres_data_dict = next(self.sqs_to_postgres.fetchDataFromSQS())
        self.assertEqual('user_id' in postgres_data_dict, True, "User ID field not present")
        self.assertEqual('device_type' in postgres_data_dict, True, "Device Type field not present")
        self.assertEqual('masked_ip' in  postgres_data_dict, True, "Masked IP field not present")
        self.assertEqual('masked_device_id' in postgres_data_dict, True, "Masked device id field not present")
        self.assertEqual('locale' in postgres_data_dict, True, "Locale field not present")
        self.assertEqual('app_version' in postgres_data_dict, True, "App version field not present")
        self.assertEqual('create_date' in postgres_data_dict, True, "Create date field not present")

    def testPostgresCredentials(self):
        '''
            Function to test when the config file is read, we have all the credential fields in the dictionary to write to postgres
        '''

        database_credentials = self.sqs_to_postgres.getPostgresCredentials('config.ini')
        self.assertEqual('database' in database_credentials, True, "Database key not present")
        self.assertEqual('user' in database_credentials, True, "User key not present")
        self.assertEqual('password' in  database_credentials, True, "Password key name not present")
        self.assertEqual(database_credentials['password']!='', True, "Password key not populated") # When the user has not added password value in config file
        self.assertEqual('host' in database_credentials, True, "Host Key not present")
        self.assertEqual('port' in database_credentials, True, "Port key not present")

    def testWriteToPostgres(self):
        '''
            Function to test when the SQS queue messages are read the fields returned in the dictionary are correct and all present for inserting into postgres database
        '''
        
        test_dict_1 = {
                        'MessageId': '123',
                        'user_id': 'asdsad=',
                        'app_version': '213',
                        'device_type': '123',
                        'masked_ip': '1234',
                        'locale': '2341',
                        'masked_device_id': '241',
                        'create_date': '214-2-1'
                      }
        
        # When all values are populated
        self.assertEqual(self.sqs_to_postgres.writeToPostgres(test_dict_1), True, "Issue with insert in postgres")

        # When one field is None
        test_dict_2 = {
                'MessageId': '123',
                'user_id': None,
                'app_version': '213',
                'device_type': '123',
                'masked_ip': '1234',
                'locale': '2341',
                'masked_device_id': '241',
                'create_date': None
                }

        self.assertEqual(self.sqs_to_postgres.writeToPostgres(test_dict_2), True, "Issue with insert in postgres")

        # When create_date has $$ value in but it is of date type
        test_dict_3 = {
                'MessageId': '123',
                'user_id': None,
                'app_version': '213',
                'device_type': '123',
                'masked_ip': '1234',
                'locale': '2341',
                'masked_device_id': '241',
                'create_date': '$$$'
                }

        self.assertEqual(self.sqs_to_postgres.writeToPostgres(test_dict_3), True, "Issue with insert in postgres")
        

sqs_read_suite = unittest.TestLoader() \
                       .loadTestsFromTestCase(TestSQSRead)

runner = unittest.TextTestRunner()
runner.run(sqs_read_suite)