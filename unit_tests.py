import unittest

from sqs_to_postgress import SQSToPostgres

sqs_to_postgres = SQSToPostgres('config.ini')

sqs_to_postgres.getSQSCredentials('config.ini')

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
        self.assertEqual(database_credentials['password']!='', True, "Password key not populated")
        self.assertEqual('host' in database_credentials, True, "Host Key not present")
        self.assertEqual('port' in database_credentials, True, "Port key not present")

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

# loads all unit tests from TestGetAreaRectangle into a test suite
sqs_read_suite = unittest.TestLoader() \
                       .loadTestsFromTestCase(TestSQSRead)

runner = unittest.TextTestRunner()
runner.run(sqs_read_suite)