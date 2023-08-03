from sqs_to_postgress import SQSToPostgres

if __name__ == "__main__":

    # create object of class SQSToPostgres and pass config file to the constructor
    sqs_to_postgres = SQSToPostgres('config.ini')

    while True:
        # Keep waiting for a message in SQS
        for data in sqs_to_postgres.fetchDataFromSQS(): # the generator will return one message and the for loop is used an iterator to iterate over the yield results
            sqs_to_postgres.writeToPostgres(data) # write the data from the generator into the postgres database