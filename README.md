# Run Debezium Kafka Connect using Docker

# **Prerequisites:**
* ##   Docker and Docker Compose installed on your machine.
* ##   Python Libraries: psycopg2, confluent-kafka.
* ##   Basic understanding of Kafka, Debezium, and PostgreSQL.


## Step 1: Create a Docker Compose File
## 1. Create a docker-compose.yml file:
## Use docker compose yml [docker-compose.yml](/docker-compose.yml) 

# Step 2: Start Docker Containers
## 1. Run Docker Compose:
## `docker-compose up -d`

# Step 3: Register Debezium PostgreSQL Connector in Kafka Connect using a REST API call:
##   To create and use a configuration JSON file in Postman for registering a Debezium connector 
##   Step 1: Open Postman
## 1. Launch Postman on your machine. If you haven’t installed it yet, download it from Postman.

##   Step 2: Create a New Request
## 1. Click on the “New” button in Postman, then choose “Request.”
## 2. Give your request a name, such as Register Debezium PostgreSQL Connector, and select a suitable collection (or create a new one).

##   Step 3: Setup Request Type and URL
## 1. Change the HTTP method to POST from the dropdown list next to the URL field.
## 2. In the URL field, enter the Kafka Connect REST endpoint. Typically, it will look like this

## `http://localhost:8083/connectors/`

## Step 4: Create a JSON Body
## 1. Click on the “Body” tab.

## 2. Select the “raw” option and then choose “JSON” from the dropdown.

## 3. Enter the configuration JSON in the editor. For example:
````aiignore
{
    "name": "inventory-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "database_hostname", # HostName
        "database.port": "database_port",# Port 
        "database.user": "database_username", # User Name
        "database.password": "database_password", # Password
        "database.dbname": "database_name", # Database Name
        "database.server.name": "dbserver1",
        "table.include.list": "table_name"
    }
}
````
# Step 4: Create a Pipeline:
## 1. Install Required Libraries:
## `pip install psycopg2-binary confluent-kafka`
## 2. Setup  Topic Name

```aiignore
from confluent_kafka import KafkaConsumer

TOPIC_NAME = 'your_topic_name'  # Topic Name  
BOOTSTRAP_SERVERS = ['localhost:9092']  # Kafka Bootstrap Servers
AUTO_OFFSET_RESET = 'earliest'
ENABLE_AUTO_COMMIT = True

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset=AUTO_OFFSET_RESET,
    group_id='sales-group'
)

```
## 3. Postgres Connection Setup
```aiignore
import psycopg2

connection = psycopg2.connect(
    dbname='your_db_name',  # Database Name
    user='your_username',  # Username
    password='your_password',  # Password
    host='localhost',  # Host Name
    port='5432'  # Port
)

```
## 4. Python Script for Initial Data Load
## Use Python script for initial Data loading [create_insert_val_tab.py](create_insert_val_tab.py) 

## 5. Python CDC Consumer for Real-time Updates:
## Use Python script for real-time CDC updates [main.py](/main.py) 
