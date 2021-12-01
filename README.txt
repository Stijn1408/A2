Data Processing Pipelines: Bitcoin-Twitter 

Objective: 
	The goal of this program is to compare the percentage change of the Bitcoin price with the sentiment of tweets on Twitter that include #Bitcoin and #BTC.

Installation:
	1. The docker-compose.yml file should be build in a virtual machine
		1.1. The name of the user and the external ip-address have to be changed to personal values
	2. In a Spark Session, the ipynb-file can be opened and executed
	3. For both the Kafka-producers, the appropriate external ip-address has to be included
	4. The ip-address also has to be changed in the kafkaadmin.py-files
	5. The producers can then be executed
	6. After permutations, the Spark Session sends the new data frames to a Google Big Query Table
		6.1. The project-id and table names may have to be changed to personal values
	7. The data can be read and visualized with Google Data Studio

