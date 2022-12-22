# Jasmin Mongo Logger

Log [Jasmin SMS Gateway](https://github.com/jookies/jasmin)'s MT/MO to MongoDB Cluster (can be a one-node cluster).


### Table of Contents
1. **[Installation Instructions](#installation-instructions)**<br>
    + **[PYPI](#pypi)**<br>
    + **[From Source](#from-source)**<br>
2. **[Setup MongoDB CLuster](#setup-mongodb-cluster)**<br>
3. **[Usage Instructions](#usage-instructions)**<br>
    + **[Data Structure](#data-structure)**<br>
    + **[Start the Service](#start-the-service)**<br>


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
### Docker:
```
docker compose -f ./docker/docker-compose.yml up -d
```
Be sure to change the `AMQP_BROKER_HOST` and `MONGO_CONNECTION_STRING` in the `docker-compose.yml` file to your desired values. and also finish the [setup of the MongoDB cluster](#setup-mongodb-cluster) before running the docker-compose file.


## Setup MongoDB CLuster
To setup a MongoDB cluster with Docker, You can use this open source [Docker Custom MongoDB Image](https://github.com/BlackOrder/mongo-cluster)


## Usage Instructions:
`Jasmin Mongo Logger` Logs all MT/MO from `jasmin` to MongoDB Cluster. All settings are read from OS ENV when run from console. if you want to import it in you code, you can supply the settings on initialization.

### Data Structure
You have to supply a database and a collection name. The package will dump all data into the collection.



### Start the service
There is multiple ways to setup the package from CLI.

1. By exporting ENV variables
you can export the fallowing variables before execution
```
AMQP_BROKER_HOST                    =              127.0.0.1
AMQP_BROKER_PORT                    =                5672
MONGO_CONNECTION_STRING             =       **REQUIRED:NoDefault**
MONGO_LOGGER_DATABASE               =       **REQUIRED:NoDefault**
MONGO_LOGGER_COLLECTION             =       **REQUIRED:NoDefault**
JASMIN_MONGO_LOGGER_LOG_PATH        =          /var/log/jasmin/
JASMIN_MONGO_LOGGER_LOG_LEVEL       =               INFO
```

Then execute:
```
jasminmongologd
```

2. You can pass arguments to the package on execution. execute ` jasminmongologd -h ` to see all possible arguments. Then execute:
```
jasminmongologd --connection-string $MONGO_CONNECTION_STRING --database $MONGO_LOGGER_DATABASE --collection $MONGO_LOGGER_COLLECTION
```

3. Mix the previous two methods. you can set the ENV variables and pass some arguments. for example:
```
AMQP_BROKER_HOST=127.0.0.1 jasminmongologd --connection-string $MONGO_CONNECTION_STRING --database $MONGO_LOGGER_DATABASE --collection $MONGO_LOGGER_COLLECTION
```
