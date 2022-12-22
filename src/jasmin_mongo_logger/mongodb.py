import logging

from pymongo import MongoClient
from pymongo.database import Database as MongoDatabase


class MongoDB:

    def __init__(self, connection_string, database_name):
        """ Constructor """
        self.connection_string = connection_string
        self.database_name = database_name
        logging.info("Starting ::MongoDB cluster connection::")

    def logger_callback(self, msg: str):
        logging.info(msg=msg)

    def startConnection(self) -> bool:
        mongoclient = MongoClient(self.connection_string)
        server_info = mongoclient.server_info()
        if isinstance(server_info, dict) and 'ok' in server_info and server_info['ok'] == 1:
            logging.info("Connected to MongoDB")
            self.mongoclient = mongoclient
            database: MongoDatabase = self.mongoclient[self.database_name]
            database_info = database.command("buildinfo")
            if isinstance(database_info, dict) and 'ok' in database_info and database_info['ok'] == 1:
                logging.info(f"Set to use database: {self.database_name}")
                logging.info("")
                self.database = database
                return True
            else:
                logging.critical(
                    f"Failed to use database: {self.database_name}")
                return False
        else:
            logging.critical("Failed to connect to MongoDB")
            return False

    def get_one_module(self, module: str) -> dict[str, str | float | bool]:
        "" ""
        data: dict[str, str | float | bool] = {}
        cursor = self.database[module].find()
        for row in cursor:
            sub_id = row["_id"]
            del row["_id"]
            data[sub_id] = row

        return data

    def get_one_submodule(self, module: str, sub_id: str) -> dict[str, str | float | bool]:
        "" ""
        return self.database[module].find_one({"_id": sub_id})

    def insert_one(self, module, sub_id, data):
        data["_id"] = sub_id
        self.database[module].insert_one(data)

    def increment_one(self, module, sub_id, data):
        self.database[module].update_one(
            {"_id": sub_id}, {'$inc': data})

    def update_one(self, module, sub_id, data, upsert=True):
        self.database[module].update_one(
            {"_id": sub_id}, {'$set': data}, upsert=upsert)
