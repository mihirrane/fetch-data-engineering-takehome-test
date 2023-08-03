import unittest

# Our code to be tested
class Rectangle:
    def __init__(self, width, height):
        self.width = width
        self.height = height

    def get_area(self):
        return self.width * self.height

    def set_width(self, width):
        self.width = width

    def set_height(self, height):
        self.height = height

# The test based on unittest module
class TestGetAreaRectangle(unittest.TestCase):
    def runTest(self):
        rectangle = Rectangle(2, 3)
        self.assertEqual(rectangle.get_area(), 6, "incorrect area")

# run the test
unittest.main()

from sqs_to_postgress import SQSToPostgres

sqs_to_postgres = SQSToPostgres('config.ini')

sqs_to_postgres.getSQSCredentials('config.ini')

# The test based on unittest module
class TestSQSRead(unittest.TestCase):
    def setUp(self):
        self.sqs_to_postgress = SQSToPostgres('config.ini')

    def testSQSCredentials(self):
        sqs_credentials_dict = self.sqs_to_postgress.getSQSCredentials('config.ini')
        self.assertEqual(sqs_credentials_dict.has_key('region_name'), True, "Region not present")
        self.assertEqual(sqs_credentials_dict.has_key('region_name'), True, "Region not present")
        self.assertEqual(sqs_credentials_dict.has_key('region_name'), True, "Region not present")
        self.assertEqual(sqs_credentials_dict.has_key('region_name'), True, "Region not present")


# loads all unit tests from TestGetAreaRectangle into a test suite
calculate_area_suite = unittest.TestLoader() \
                       .loadTestsFromTestCase(TestGetAreaRectangleWithSetUp)

runner = unittest.TextTestRunner()
runner.run(calculate_area_suite)


for data in sqs_to_postgres.fetchDataFromSQS():
