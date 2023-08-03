## Design approach

1. Fetching from SQS table :

	I chose to read one message at a time from the queue, flatten the message into a python dictionary. The keys of python dictionary are the columns that are in the user\_logins table. While creating the dictionary, I applied SHA512 hashing on the PII fields.

	Once, the dictionary is created, I delete the message from the queue.

2. Writing to the postgres table

	The dictionary from previous process is the input to this function. From this dictionary, I write the values as a record in the postgres database

3. Putting it all together

	I call fetchDataFromSQS() function and it returns dictionary with one record and I write the record into the postgres database using function writeToPostgres(). fetchDataFromSQS() is a generator function which returns one dictionary at a time.

	I continuously poll the queue so that in event of new message getting added to the queue, I can quickly write it to the postgres database. This loop will run infinitely and the process never suspends.

## Questions on the decisions made:

1. How will you read messages from the queue?

	I created a generator function fetchDataFromSQS() which generates a dictionary containing all column values. If a particular column value is not present in the SQS message, the value is None in the dictionary

2. What type of data structures should be used?

	I have used a dictionary to represent the message from SQS queue

3. How will you mask the PII data so that duplicate values can be identified?
 	
 	To mask the PII data, I chose to use the SHA512 hashing because hashing techniques are irreversible. This offers protection to the data. Duplicate values can be identified as same value will always result in the same hashed result.

4. What will be your strategy for connecting and writing to Postgres?

	For writing to Postgres database, I used the library psycopg2 using which I created connection, execute SQL query to insert the record, commit the results and finally, close the connection. Since, from the table definition all columns allow Null data, if any of the fields from the message is Null it will be inserting into the postgres table

5. Where and how will your application run?

	The application can be run using the docker compose.yml file provided. I have listed the steps to run below.

## Steps to run code

1. Clone the git repository
 	
 	`git clone https://github.com/mihirrane/fetch-data-engineering-takehome-test.git`


2. Open config.ini and add values for password in postgres credentials

3. Navigate to the folder in which the repository is cloned. Open command line inside the folder and run<br>
 `docker compose up`

4. Check the container id of the service which is running the python script. Open another command line within the folder and run <br>
 `docker ps` <br>
![](https://github.com/mihirrane/fetch-data-engineering-takehome-test/blob/main/images/docker_ps.png)

 Copy the container id corresponding to the image fetch-sqs\_to\_postgres


5. The container is running infinitely but I need to enter inside the container to run the python script<br>
 `docker exec -it container_id bash`


6. Bash shell will open. Run command<br>
   `python main.py`


7. The data will be written to postgres. To check, note the container id of postgres container, <br>
   `fetchdocker/data-takehome-postgres`


8. Run command <br>
   `docker exec -it container_id bash`


9. Login to postgres using command:<br>
   `psql -d postgres -U postgres -p 5432 -h localhost -W`

   Type password which is given in the provided document


10. When the credentials are accepted, postgress command shell will open


11. Type the query -<br>
    `select \* from user\_logins;`<br>
 	You will see all the rows, 100 in our case

## Questions asked

1. How would you deploy this application in production?

	The application is hosted on docker hub. I need to build a deployment configuration using docker-compose.yml. Then I need to use a container orchestration tool such as docker swarm or Kubernetes to monitor the docker containers. I need a host for these docker containers such as AWS, Azure or any other cloud services.

	I attempted to have the code in a separate docker image from the docker image of SQS and Postgres but I were finding it difficult to make the docker containers communicate with each other. I tried using 'depends\_on' and 'links' in the docker compose file but it did not work.

	I also tried using linking the networks: default in SQS and python script container. It did not work. In production, the SQS link will not have 'localhost' in it. It will be deployed on AWS server and thus, this network issue will not persist.

	![](https://github.com/mihirrane/fetch-data-engineering-takehome-test/blob/main/images/queue_issue.png)

2. What other components would you want to add to make this production ready?

	To make this production ready, the AWS SQS will not be hosted on localhost and the python script will be accessed from its image and not using the volume mount. Ideally, each piece should be its separate image and they should communicate with each other through the docker compose file.

3. How can this application scale with a growing dataset.

	The application reads one message at a time from SQS. With a growing dataset, it will still read one message, so the performance is unaffected with scale. But instead of using SQS, if Kafka were used as a system to store the message, I can use multiple workers and create partitions to process messages faster.

4. How can PII be recovered later on?
 
 	I can use different encryption algorithms such as RSA, AES, Blowfish to encrypt the data. With the usage of private key only the person who has access can see the original value of the fields.

5. What are the assumptions you made?

	- AWS credentials were not provided so I added dummy credentials -
 	  `aws\_access\_key\_id=foobar`
	  `aws\_secret\_access\_key=foobar`

      The error I got was - botocore.exceptions.NoCredentialsError: Unable to locate credentials

	- Region for the AWS server was not provided
 		So I added `region\_name=us-east-1`

	  The error I got was botocore.exceptions.NoRegionError: You must specify a region.

	- For app version, from the DDL I understood its integer column so I selected the first number before the first dot to assign to app\_version. For eg. 23.0.1 will be 23

	- Since all columns are nullable if the 'body' key is present in the message, I will insert the values of all columns within it. If the body Json is empty, I will insert all values as Null
	
	- If the body is not present in the message JSON, no insertion will be made and the log will record the messageId for future reference
	
	- The wait time is set to 20 seconds to wait for a message from SQS. After that control will enter the infinite loop and again attempt for 20 seconds

## Folder contents

1. config.ini
   
   Contains the configuration details for the postgres database and the SQS credentials

3. main.py

   Python script which reads the credentials from config.ini and calls the methods from SQSToPostgres class to read data from SQS and write to postgres

4. unit\_tests.py
	
   Python script (unit\_tests.py) which consists of all unit tests

5. utils

   Python script (sqs_to_postgress) script which contains the class SQSToPostgres. The methods in this class are encode(), decode(), fetchDataFromSQS() and writeToPostgres()

5. requirements.txt

   File contains the details of all libraries that are used in all code files

6. Dockerfile

   Dockerfile contains instructions to create the docker image