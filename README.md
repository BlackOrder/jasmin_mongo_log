# Jasmin Mongo Logger

Log [Jasmin SMS Gateway](https://github.com/jookies/jasmin)'s MT/MO to MongoDB Cluster (can be a one-node cluster).


### Table of Contents
1. **[Installation Instructions](#installation-instructions)**<br>
    + **[PYPI](#pypi)**<br>
    + **[From Source](#from-source)**<br>
2. **[Setup MongoDB CLuster](#setup-mongodb-cluster)**<br>
    + **[Docker](#docker)**<br>
        * **[Prepare network](#prepare-network)**<br>
        * **[Prepare variables](#prepare-variables)**<br>
        * **[Create cluster key](#create-cluster-key)**<br>
            1. **[One line](#one-line)**<br>
            2. **[Manually](#manually)**<br>
        * **[Start MongoDB cluster service](#start-mongodb-cluster-service)**<br>
3. **[Usage Instructions](#usage-instructions)**<br>
    + **[Data Structure](#data-structure)**<br>
    + **[Start the Service](#start-the-service)**<br>
        * **[Set variables](#set-variables)**<br>
        * **[Start Change Stream](#start-change-stream)**<br>


## Installation Instructions
### PYPI:
```
pip3 install -U jasmin-mongo-logger
```
### From Source:
```
git clone https://github.com/BlackOrder/jasmin_mongo_logger.git
cd jasmin_mongo_logger
pip3 install .
```

## Setup MongoDB CLuster
### Docker
#### Prepare network:
```
docker network create mongo_cluster_subnet
```

#### Prepare variables:
```
cd docker
cp .env.example .env
```
Edit the `.env` file. change the default username and password.

#### Create cluster key:
Mongo nodes need to share the same key to be able to authenticate each-other


##### One line:
```
echo "MONGODB_CLUSTER_KEY="$(openssl rand -base64 756 | sed -z 's/\n/\\n/g') >> .env
```
if the above failes to create the env variable in `.env` file, you have to do it in multiple steps.


##### Manually
1. Create key file:
```
openssl rand -base64 756 > mongo_cluster_key
```

2. Edit Key:
```
vim ./mongo_cluster_key
```
replace all new-line with `\n`

3. Step 3:
Create a variable in `.env` file and use the string from `Step 2` as it's value
```
MONGODB_CLUSTER_KEY=####-Single line encrypted key-####
```

#### Start MongoDB cluster service:
```
docker compose -f docker-compose.MongoCluster.yml up -d
```


## Usage Instructions:
`Jasmin Mongo Logger` Logs all MT/MO from `jasmin` to MongoDB Cluster. All settings are read from OS ENV when run from console. if you want to import it in you code, you can supply the settings on initialization.

### Data Structure
You have to supply a database and a collection name. The package will dump all data into the collection.


### Start the service
#### Set variables
To start the package. you have to export the fallowing variables:
```
AMQP_BROKER_HOST                        =             127.0.0.1
AMQP_BROKER_PORT                        =               5672
MONGODB_CONNECTION_STRING               =       **REQUIRED:NoDefault**
MONGODB_LOGS_DATABASE                   =       **REQUIRED:NoDefault**
MONGODB_LOGS_COLLECTION                 =       **REQUIRED:NoDefault**
JASMIN_MONGO_CONFIGURATION_LOG_PATH     =             /var/log
```
#### Start Log-Reactor
```
jasminmongologd
```
