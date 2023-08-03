from sqs_to_postgress import SQSToPostgres

if __name__ == "__main__":

    sqs_to_postgres = SQSToPostgres('config.ini')

    while True:
        for data in sqs_to_postgres.fetchDataFromSQS()):
            sqs_to_postgres.writeToPostgres(data)