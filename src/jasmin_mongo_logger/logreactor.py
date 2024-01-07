import binascii
from copy import deepcopy
import logging
import logging.handlers
import os
import pickle as pickle
from datetime import datetime
import sys
from time import sleep
import argparse
import pkg_resources
import txamqp.spec
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.internet.protocol import ClientCreator
from txamqp.client import TwistedDelegate
from txamqp.protocol import AMQClient
from smpp.pdu.pdu_types import EsmClassGsmFeatures, DataCodingDefault
from smpp.pdu.constants import data_coding_default_name_map

from .mongodb import MongoDB

# get the package name this script is running from
package_name = __name__.split(".")[0]
package_version = pkg_resources.get_distribution(package_name).version

NODEFAULT: str = "REQUIRED: NO_DEFAULT"
DEFAULT_QUEUE_NAME: str = "%s_queue" % package_name
DEFAULT_EXCHANGE_NAME: str = "messaging"
DEFAULT_CONSUMER_TAG: str = "%s_consumer" % package_name
DEFAULT_RETRY_ON_CONNECTION_ERROR: bool = os.getenv(
    "RETRY_ON_CONNECTION_ERROR", "True"
).lower() in (
    "yes",
    "true",
    "t",
    "1",
)
DEFAULT_MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "0"))
DEFAULT_RETRY_DELAY: int = int(os.getenv("RETRY_DELAY", "5"))

DEFAULT_AMQP_BROKER_HOST: str = os.getenv("AMQP_BROKER_HOST", "127.0.0.1")
DEFAULT_AMQP_BROKER_PORT: int = int(os.getenv("AMQP_BROKER_PORT", "5672"))
DEFAULT_AMQP_BROKER_VHOST: str = os.getenv("AMQP_BROKER_VHOST", "/")
DEFAULT_AMQP_BROKER_USERNAME: str = os.getenv("AMQP_BROKER_USERNAME", "guest")
DEFAULT_AMQP_BROKER_PASSWORD: str = os.getenv("AMQP_BROKER_PASSWORD", "guest")
DEFAULT_AMQP_BROKER_HEARTBEAT: int = int(os.getenv("AMQP_BROKER_HEARTBEAT", "0"))

DEFUALT_LOGGER_PRIVACY: bool = os.getenv(
    "JASMIN_MONGO_LOGGER_PRIVACY", "False"
).lower() in (
    "yes",
    "true",
    "t",
    "1",
)

DEFAULT_LOG_LEVEL: str = os.getenv("JASMIN_MONGO_LOGGER_LOG_LEVEL", "INFO").upper()
DEFAULT_LOG_PATH: str = os.getenv("JASMIN_MONGO_LOGGER_LOG_PATH", "/var/log/jasmin")
DEFAULT_LOG_FILE: str = os.getenv(
    "JASMIN_MONGO_LOGGER_LOG_FILE", "%s.log" % package_name
)
DEFAULT_LOG_ROTATE: str = os.getenv("JASMIN_MONGO_LOGGER_LOG_ROTATE", "midnight")
DEFAULT_LOG_FORMAT: str = os.getenv(
    "JASMIN_MONGO_LOGGER_LOG_FORMAT",
    "%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  ** %(message)-55s **",
)
DEFAULT_LOG_DATE_FORMAT: str = os.getenv(
    "JASMIN_MONGO_LOGGER_LOG_DATE_FORMAT", "%Y-%m-%d %H:%M:%S"
)
DEFAULT_FILE_LOGGING: bool = os.getenv(
    "JASMIN_MONGO_LOGGER_FILE_LOGGING", "True"
).lower() in ("yes", "true", "t", "1")
DEFAULT_CONSOLE_LOGGING: bool = os.getenv(
    "JASMIN_MONGO_LOGGER_CONSOLE_LOGGING", "True"
).lower() in ("yes", "true", "t", "1")


class LogReactor:
    def __init__(
        self,
        mongo_connection_string: str,
        logger_database: str,
        logger_collection: str,
        logger_privacy: bool = DEFUALT_LOGGER_PRIVACY,
        amqp_broker_host: str = DEFAULT_AMQP_BROKER_HOST,
        amqp_broker_port: int = DEFAULT_AMQP_BROKER_PORT,
        amqp_broker_vhost: str = DEFAULT_AMQP_BROKER_VHOST,
        amqp_broker_username: str = DEFAULT_AMQP_BROKER_USERNAME,
        amqp_broker_password: str = DEFAULT_AMQP_BROKER_PASSWORD,
        amqp_broker_heartbeat: int = DEFAULT_AMQP_BROKER_HEARTBEAT,
        retry_on_connection_error: bool = DEFAULT_RETRY_ON_CONNECTION_ERROR,
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_delay: int = DEFAULT_RETRY_DELAY,
        log_level: str = DEFAULT_LOG_LEVEL,
        log_path: str = DEFAULT_LOG_PATH,
        log_file: str = DEFAULT_LOG_FILE,
        log_rotate: str = DEFAULT_LOG_ROTATE,
        file_logging: bool = DEFAULT_FILE_LOGGING,
        console_logging: bool = DEFAULT_CONSOLE_LOGGING,
    ):
        self.RETRY_ON_CONNECTION_ERROR = retry_on_connection_error
        self.amqp_broker_max_retries, self.mongo_max_retries = max_retries, max_retries
        self.RETRY_DELAY = retry_delay
        self.RETRY_FOREVER = max_retries <= 0

        self.AMQP_BROKER_HOST = amqp_broker_host
        self.AMQP_BROKER_PORT = amqp_broker_port
        self.AMQP_BROKER_VHOST = amqp_broker_vhost
        self.AMQP_BROKER_USERNAME = amqp_broker_username
        self.AMQP_BROKER_PASSWORD = amqp_broker_password
        self.AMQP_BROKER_HEARTBEAT = amqp_broker_heartbeat

        self.MONGO_CONNECTION_STRING = mongo_connection_string
        self.MONGO_LOGGER_DATABASE = logger_database
        self.MONGO_LOGGER_COLLECTION = logger_collection

        self.LOGGER_PRIVACY = logger_privacy

        self.LOG_LEVEL = log_level
        self.LOG_PATH = log_path
        self.LOG_FILE = log_file
        self.LOG_ROTATE = log_rotate
        self.FILE_LOGGING = file_logging
        self.CONSOLE_LOGGING = console_logging

        # Set up logging
        self.LOG_FORMAT = os.getenv(
            "JASMIN_MONGO_LOGGER_LOG_FORMAT", DEFAULT_LOG_FORMAT
        )
        self.LOG_DATE_FORMAT = os.getenv(
            "JASMIN_MONGO_LOGGER_LOG_DATE_FORMAT", DEFAULT_LOG_DATE_FORMAT
        )

        # Enable logging if console logging or file logging is enabled
        if self.FILE_LOGGING or self.CONSOLE_LOGGING:
            logFormatter = logging.Formatter(
                self.LOG_FORMAT, datefmt=self.LOG_DATE_FORMAT
            )
            rootLogger = logging.getLogger()
            rootLogger.setLevel(self.LOG_LEVEL)

            # add the handler to the root logger if enabled
            if self.CONSOLE_LOGGING:
                consoleHandler = logging.StreamHandler(sys.stdout)
                consoleHandler.setFormatter(logFormatter)
                rootLogger.addHandler(consoleHandler)
                logging.debug("Logging to console")

            # add the handler to the root logger if enabled
            if self.FILE_LOGGING:
                if not os.path.exists(self.LOG_PATH):
                    os.makedirs(self.LOG_PATH)

                fileHandler = logging.handlers.TimedRotatingFileHandler(
                    filename="%s/%s"
                    % (self.LOG_PATH.rstrip("/"), self.LOG_FILE.lstrip("/")),
                    when=self.LOG_ROTATE,
                )
                fileHandler.setFormatter(logFormatter)
                rootLogger.addHandler(fileHandler)
                logging.debug("Logging to file: %s/%s" % (self.LOG_PATH, self.LOG_FILE))
        # Disable logging if console logging and file logging are disabled
        else:
            logging.disable(logging.CRITICAL)

    def startReactor(self):
        logging.info("*********************************************")
        logging.info("*********************************************")
        logging.info(f"::Jasmin MongoDB Logger v{package_version}::")
        logging.info(" ")
        logging.info("::Configuration::")
        logging.info(f"AMQP Broker Host: {self.AMQP_BROKER_HOST}")
        logging.info(f"AMQP Broker Port: {self.AMQP_BROKER_PORT}")
        logging.info(f"AMQP Broker VHost: {self.AMQP_BROKER_VHOST}")
        logging.info(f"AMQP Broker Username: {self.AMQP_BROKER_USERNAME}")
        logging.info(f"AMQP Broker Password: {self.AMQP_BROKER_PASSWORD}")
        logging.info(f"AMQP Broker Heartbeat: {self.AMQP_BROKER_HEARTBEAT}")
        logging.info(
            f"Retry on connection error: {'Yes' if self.RETRY_ON_CONNECTION_ERROR else 'No'}"
        )
        logging.info(
            f"Retry count: {'Forever' if self.RETRY_FOREVER else self.amqp_broker_max_retries}"
        )
        logging.info(f"Retry timeout: {self.RETRY_DELAY}s")
        logging.info(f"MongoDB Logs Database: {self.MONGO_LOGGER_DATABASE}")
        logging.info(f"MongoDB Logs Collection: {self.MONGO_LOGGER_COLLECTION}")
        logging.info(f"Log Level: {self.LOG_LEVEL}")
        logging.info(f"Log Path: {self.LOG_PATH}")
        logging.info(f"Log File: {self.LOG_FILE}")
        logging.info(f"Log Rotate: {self.LOG_ROTATE}")
        logging.info(f"File Logging: {'Enabled' if self.FILE_LOGGING else 'Disabled'}")
        logging.info(
            f"Console Logging: {'Enabled' if self.CONSOLE_LOGGING else 'Disabled'}"
        )
        logging.info("*********************************************")

        # Connect to RabbitMQ
        self.rabbitMQConnect()

        # Run the reactor
        logging.debug("Running reactor")
        reactor.run()

    @inlineCallbacks
    def gotConnection(self, conn, username, password):
        logging.info("---------------------------------------------")
        logging.info(f"Connected to broker, authenticating: {username}")

        yield conn.start({"LOGIN": username, "PASSWORD": password})

        logging.info("Authenticated!")
        logging.debug("Set up channel")
        chan = yield conn.channel(1)

        # Needed to clean up the connection
        logging.debug("Cleaning up ...")
        self.conn = conn
        self.chan = chan

        logging.debug("Opening channel")
        yield chan.channel_open()
        logging.debug("Channel opened")

        logging.debug("Declaring queue")
        yield chan.queue_declare(queue=DEFAULT_QUEUE_NAME)
        logging.debug("Queue declared")

        # Bind to submit.sm.* routes
        logging.debug("Binding to submit.sm.* routes")
        yield chan.queue_bind(
            queue=DEFAULT_QUEUE_NAME,
            exchange=DEFAULT_EXCHANGE_NAME,
            routing_key="submit.sm.*",
        )
        logging.debug("Bound to submit.sm.resp.*")

        # Bind to submit.sm.resp.* routes
        logging.debug("Binding to submit.sm.resp.* route")
        yield chan.queue_bind(
            queue=DEFAULT_QUEUE_NAME,
            exchange=DEFAULT_EXCHANGE_NAME,
            routing_key="submit.sm.resp.*",
        )
        logging.debug("Bound to submit.sm.resp.*")

        logging.debug("Binding to dlr_thrower.* route")
        # Bind to dlr_thrower.* to track DLRs
        yield chan.queue_bind(
            queue=DEFAULT_QUEUE_NAME,
            exchange=DEFAULT_EXCHANGE_NAME,
            routing_key="dlr_thrower.*",
        )
        logging.debug("Queue bound")

        logging.debug("Starting consumer")
        yield chan.basic_consume(
            queue=DEFAULT_QUEUE_NAME, no_ack=False, consumer_tag=DEFAULT_CONSUMER_TAG
        )
        logging.debug("Consumer started")

        queue = yield conn.queue(DEFAULT_CONSUMER_TAG)

        # Connect to MongoDB
        logging.debug("Connecting to MongoDB")
        mongosource = self._connect_to_mongo(
            connection_string=self.MONGO_CONNECTION_STRING,
            database_name=self.MONGO_LOGGER_DATABASE,
        )

        # Retry connection if failed
        while (
            mongosource is None
            and self.RETRY_ON_CONNECTION_ERROR
            and (self.mongo_max_retries > 0 or self.RETRY_FOREVER)
        ):
            logging.info(f"Reconnecting in {self.RETRY_DELAY} seconds ...")
            sleep(self.RETRY_DELAY)
            mongosource = self._connect_to_mongo(
                connection_string=self.MONGO_CONNECTION_STRING,
                database_name=self.MONGO_LOGGER_DATABASE,
            )
            self.mongo_max_retries -= 1

        # Check if mongosource is None, if so, stop reactor
        if mongosource is None:
            logging.critical("MongoDB connection failed: no more retries")
            self.StopReactor()
            return

        logging.debug("MongoDB connection passed")
        # Wait for messages
        # This can be done through a callback ...
        logging.debug("Starting message processing")

        try:
            logging.debug("Starting Daemon")
            while True:
                logging.debug("Waiting for messages")
                msg = yield queue.get()

                logging.debug("Got message")
                # Get message properties
                props = msg.content.properties
                headers = props.get("headers")
                message_id = props.get("message-id")

                logging.debug("Processing message")
                logging.debug(f"Message ID: {message_id}")
                logging.debug(f"Routing key: {msg.routing_key}")
                logging.debug(f"Headers: {headers}")
                logging.debug(f"Payload: {msg.content.body}")
                logging.debug(" ")

                if (
                    msg.routing_key[:10] == "submit.sm."
                    and msg.routing_key[:15] != "submit.sm.resp."
                ):
                    # It's a submit_sm
                    logging.debug("It's a submit_sm***")
                    created_at = headers.get("created_at")
                    priority = props.get("priority")
                    source = headers.get("source_connector")
                    route = msg.routing_key[10:]
                    pdu = pickle.loads(msg.content.body)

                    logging.debug(f"message-id: {message_id}")
                    logging.debug(f"created_at: {created_at}")
                    logging.debug(f"priority: {priority}")
                    logging.debug(f"source: {source}")
                    logging.debug(f"route: {route}")

                    pdu_data = pdu.params
                    destination_addr = pdu_data.get("destination_addr").decode("utf-8")
                    source_addr = pdu_data.get("source_addr").decode("utf-8")
                    schedule_delivery_time = pdu_data.get("schedule_delivery_time")
                    validity_period = pdu_data.get("validity_period")
                    data_coding = pdu_data.get("data_coding")
                    validity = (
                        None
                        if ("headers" not in props or "expiration" not in headers)
                        else headers.get("expiration")
                    )
                    status = pdu.status

                    sms_pages = 1  # TODO: calculate sms_pages
                    short_message = None

                    UDHI_INDICATOR_SET = False
                    if hasattr(pdu_data.get("esm_class"), "gsmFeatures"):
                        for gsmFeature in pdu_data.get("esm_class").gsmFeatures:
                            if gsmFeature == EsmClassGsmFeatures.UDHI_INDICATOR_SET:
                                UDHI_INDICATOR_SET = True
                                break

                    # What type of splitting ?
                    splitMethod = None
                    if "sar_msg_ref_num" in pdu_data:
                        splitMethod = "sar"
                    elif (
                        UDHI_INDICATOR_SET
                        and pdu_data.get("short_message")[:3] == b"\x05\x00\x03"
                    ):
                        splitMethod = "udh"

                    logging.debug(f"splitMethod: {splitMethod}")
                    logging.debug(f"UDHI_INDICATOR_SET: {UDHI_INDICATOR_SET}")

                    # Concatenate short_message
                    if splitMethod is not None:
                        if splitMethod == "sar":
                            short_message = pdu_data.get("short_message")
                        else:
                            short_message = pdu_data.get("short_message")[6:]

                        while hasattr(pdu, "nextPdu"):
                            pdu = pdu.nextPdu
                            pdu_data = pdu.params
                            if splitMethod == "sar":
                                short_message += pdu_data.get("short_message")
                            else:
                                short_message += pdu_data.get("short_message")[6:]

                            sms_pages += 1
                    else:
                        short_message = pdu_data.get("short_message")

                    # Save short_message bytes
                    binary_message = binascii.hexlify(short_message)

                    # Decode short_message
                    short_message_decoded = short_message
                    if data_coding is not None:
                        if data_coding in [
                            data_coding_default_name_map.get(
                                DataCodingDefault.SMSC_DEFAULT_ALPHABET.name
                            ),
                            data_coding_default_name_map.get(
                                DataCodingDefault.IA5_ASCII.name
                            ),
                        ]:
                            short_message_decoded = short_message.decode(
                                "ascii", "replace"
                            )
                        elif data_coding == data_coding_default_name_map.get(
                            DataCodingDefault.LATIN_1.name
                        ):
                            short_message_decoded = short_message.decode(
                                "latin_1", "replace"
                            )
                        elif data_coding == data_coding_default_name_map.get(
                            DataCodingDefault.UCS2.name
                        ):
                            short_message_decoded = short_message.decode(
                                "UTF-16BE", "replace"
                            )
                        else:
                            short_message_decoded = short_message.decode(
                                "UTF-8", "replace"
                            )

                    private_short_message = '** %s byte content **' % len(short_message)
                    private_binary_message = '** %s byte content **' % len(binary_message)
                    private_short_message_decoded = '** %s char content **' % len(short_message_decoded)

                    logging.debug(f"short_message: {short_message}")
                    logging.debug(f"short_message_binary: {binary_message}")
                    logging.debug(f"short_message_decoded: {short_message_decoded}")

                    logging.debug(f"short_message: (privacy ON): {private_short_message}")
                    logging.debug(f"short_message_binary: (privacy ON): {private_binary_message}")
                    logging.debug(f"short_message_decoded: (privacy ON): {private_short_message_decoded}")

                    logging.debug(f"destination_addr: {destination_addr}")
                    logging.debug(f"source_addr: {source_addr}")
                    logging.debug(f"schedule_delivery_time: {schedule_delivery_time}")
                    logging.debug(f"validity_period: {validity_period}")
                    logging.debug(f"data_coding: {data_coding}")
                    logging.debug(f"validity: {validity}")
                    logging.debug(f"status: {status}")
                    logging.debug(f"sms_pages: {sms_pages}")

                    billing_pickle = headers.get("submit_sm_resp_bill")
                    if not billing_pickle:
                        billing_pickle = headers.get("submit_sm_bill")

                    billing = pickle.loads(billing_pickle)

                    bill: dict = {
                        "_id": billing.bid,
                        "user": {
                            "_id": billing.user.uid,
                            "group": billing.user.group.gid,
                            "username": billing.user.username,
                            "quota": {
                                "balance": billing.user.mt_credential.quotas.get(
                                    "balance"
                                ),
                                "submit_sm_count": billing.user.mt_credential.quotas.get(
                                    "submit_sm_count"
                                ),
                            },
                        },
                        "source_connector": source,
                        "routed_cid": route,
                        "created_at": created_at,
                        "priority": priority,
                        "destination_addr": destination_addr,
                        "source_addr": source_addr,
                        "schedule_delivery_time": schedule_delivery_time,
                        "validity_period": validity_period,
                        "page_count": sms_pages,
                        "amount_rate": billing.getTotalAmounts(),
                        "amount_charge": billing.getTotalAmounts() * sms_pages,
                        "sms_count_rate": billing.actions.get(
                            "decrement_submit_sm_count"
                        ),
                        "sms_count_charge": billing.actions.get(
                            "decrement_submit_sm_count"
                        )
                        * sms_pages,
                    }

                    logging.debug("**** submit_sm_bill:")
                    logging.debug("bill:")
                    # log formated bill dict
                    for key, value in bill.items():
                        logging.debug(f"\t{key}: {value}")


                    # Save message in MongoDB
                    logging.debug("Saving message in MongoDB")
                    mongosource.update_one(
                        module=self.MONGO_LOGGER_COLLECTION,
                        sub_id=message_id,
                        data={
                            "created_at": created_at,
                            "priority": priority,
                            "source":source,
                            "route":route,
                            "destination_addr": destination_addr,
                            "source_addr": source_addr,
                            "schedule_delivery_time": schedule_delivery_time,
                            "validity_period": validity_period,
                            "data_coding":data_coding,
                            "validity":validity,
                            "status":status,
                            "page_count": sms_pages,
                            "short_message": short_message if not self.LOGGER_PRIVACY else private_short_message,
                            "binary_message": binary_message if not self.LOGGER_PRIVACY else private_binary_message,
                            "short_message_decoded": short_message_decoded if not self.LOGGER_PRIVACY else private_short_message_decoded,
                            "bill": bill,
                        },
                    )
                elif msg.routing_key[:15] == "submit.sm.resp.":
                    # It's a submit_sm_resp
                    logging.debug("It's a submit_sm_resp")

                    pdu = pickle.loads(msg.content.body)

                    # get qmsg from MongoDB
                    logging.debug("Getting message info from MongoDB")
                    qmsg = mongosource.get_one_submodule(
                        module=self.MONGO_LOGGER_COLLECTION,
                        sub_id=props["message-id"],
                    )

                    # Check if qmsg is None
                    if qmsg is None:
                        logging.error(
                            f"resp to an unknown: {props['message-id']}"
                        )
                        chan.basic_ack(delivery_tag=msg.delivery_tag)
                        continue

                    if qmsg["source_addr"] is None:
                        qmsg["source_addr"] = ""

                    if qmsg["destination_addr"] is None:
                        qmsg["destination_addr"] = ""

                    if qmsg["short_message"] is None:
                        qmsg["short_message"] = ""

                    # Update message status
                    logging.debug("Updating message status in MongoDB")
                    logging.debug("current msg object: ")
                    logging.debug(qmsg)
                    logging.debug("received submit_sm_resp: ")
                    logging.debug(pdu.status)

                    # Update message status
                    logging.debug("Updating message info in MongoDB")
                    mongosource.update_one(
                        module=self.MONGO_LOGGER_COLLECTION,
                        sub_id=props["message-id"],
                        data={
                            "source_addr": qmsg["source_addr"],
                            "rate": qmsg["rate"],
                            "pdu_count": qmsg["pdu_count"],
                            "charge": qmsg["charge"],
                            "destination_addr": qmsg["destination_addr"],
                            "short_message": qmsg["short_message"],
                            "status": pdu.status,
                            "uid": qmsg["uid"],
                            "created_at": props["headers"]["created_at"],
                            "binary_message": qmsg["binary_message"],
                            "routed_cid": qmsg["routed_cid"],
                            "source_connector": qmsg["source_connector"],
                            "status_at": props["headers"]["created_at"],
                        },
                    )

                elif msg.routing_key[:12] == "dlr_thrower.":
                    # It's a dlr_thrower
                    logging.debug("It's a dlr_thrower")

                    if props["headers"]["message_status"][:5] == "ESME_":
                        # Ignore dlr from submit_sm_resp
                        logging.debug("Ignoring dlr from submit_sm_resp")
                        chan.basic_ack(delivery_tag=msg.delivery_tag)
                        continue

                    # It's a dlr
                    logging.debug("It's a dlr")
                    
                    # get qmsg from MongoDB
                    logging.debug("Getting message info from MongoDB")
                    qmsg = mongosource.get_one_submodule(
                        module=self.MONGO_LOGGER_COLLECTION,
                        sub_id=props["message-id"],
                    )
                    
                    # Check if qmsg is None
                    if qmsg is None:
                        logging.error(
                            f"dlr to an unknown: {props['message-id']}"
                        )
                        chan.basic_ack(delivery_tag=msg.delivery_tag)
                        continue

                    # Update message status
                    logging.debug("Updating message status in MongoDB")
                    logging.debug("current msg object: ")
                    logging.debug(qmsg)
                    logging.debug("received dlr: ")
                    logging.debug(props["headers"]["message_status"])

                    mongosource.update_one(
                        module=self.MONGO_LOGGER_COLLECTION,
                        sub_id=props["message-id"],
                        data={
                            "status": props["headers"]["message_status"],
                            "status_at": datetime.now(),
                        },
                    )

                else:
                    logging.error(f"unknown route: {msg.routing_key}")

                chan.basic_ack(delivery_tag=msg.delivery_tag)
                logging.debug("Message processed")
                logging.debug(" ")

        except KeyboardInterrupt:
            logging.critical("User Terminated")
            # mark as do not reconnect
            self.RETRY_ON_CONNECTION_ERROR = False
        except Exception as err:
            logging.critical("Unknown Error")
            logging.debug("Exception:")
            logging.debug(err)
        except:
            logging.critical("Unknown Error")

        # check if we should reconnect
        if not self.RETRY_ON_CONNECTION_ERROR or (
            self.amqp_broker_max_retries <= 0 and not self.RETRY_FOREVER
        ):
            self.StopReactor()
            return

        # decrement retry count
        self.amqp_broker_max_retries -= 1

        # clean up
        logging.debug("Cleaning up")
        self.cleanConnectionBreak()
        logging.debug("Cleaning up done")

        # Restart the connection in RETRY_DELAY seconds
        logging.info(f"Reconnecting in {self.RETRY_DELAY} seconds ...")
        try:
            yield reactor.callLater(self.RETRY_DELAY, self.rabbitMQConnect)
        except KeyboardInterrupt:
            logging.critical("User Terminated")
            self.StopReactor()

    def _connect_to_mongo(
        self, connection_string: str, database_name: str
    ) -> MongoDB | None:
        mongosource = MongoDB(
            connection_string=connection_string,
            database_name=database_name,
        )

        logging.debug("Checking MongoDB connection")
        if mongosource.startConnection() is not True:
            logging.info("MongoDB connection failed")
            return None
        else:
            logging.info("MongoDB connection successful")
            return mongosource

    @inlineCallbacks
    def ConError(self, err):
        logging.critical("RabbitMQ connection error")
        logging.debug("Exception:")
        logging.debug(err)
        self.cleanConnectionBreak()

        # check if we should reconnect
        if not self.RETRY_ON_CONNECTION_ERROR or (
            self.amqp_broker_max_retries <= 0 and not self.RETRY_FOREVER
        ):
            self.StopReactor()
            return

        # decrement retry count
        self.amqp_broker_max_retries -= 1

        # Wait for RETRY_DELAY seconds before trying to reconnect, but listen for Ctrl+C
        logging.info(f"Reconnecting in {self.RETRY_DELAY} seconds ...")
        try:
            yield reactor.callLater(self.RETRY_DELAY, self.rabbitMQConnect)
        except KeyboardInterrupt:
            logging.critical("User Terminated")
            self.StopReactor()

    def cleanConnectionBreak(self):
        # A clean way to tear down and stop
        logging.debug("Cleaning up connection")
        yield self.chan.basic_cancel(DEFAULT_CONSUMER_TAG)
        logging.debug("Closing channel")
        yield self.chan.channel_close()
        logging.debug("Closing channel 0")
        chan0 = yield self.conn.channel(0)
        logging.debug("Closing connection")
        yield chan0.connection_close()
        logging.debug("Cleaning up done")

    def StopReactor(self):
        logging.critical("Shutting down !!!")
        logging.critical("Cleaning up ...")

        self.cleanConnectionBreak()
        logging.debug("Connection closed")

        logging.debug("Stopping reactor")
        if reactor.running:
            logging.debug("Stopping reactor")
            reactor.stop()

        logging.debug("Waiting for reactor to stop")
        sleep(3)
        logging.debug("Reactor stopped")

    def rabbitMQConnect(self):
        # Connect to RabbitMQ
        logging.debug("---------------------------------------------")
        logging.debug("Creating a new RabbitMQ connection")
        host = self.AMQP_BROKER_HOST
        port = self.AMQP_BROKER_PORT
        vhost = self.AMQP_BROKER_VHOST
        username = self.AMQP_BROKER_USERNAME
        password = self.AMQP_BROKER_PASSWORD
        heartbeat = self.AMQP_BROKER_HEARTBEAT

        logging.debug(
            f"Credentials:\n\
            Host: {host}\n\
            Port: {port}\n\
            Vhost: {vhost}\n\
            Username: {username}\n\
            Password: {password}\n\
            Heartbeat: {heartbeat}"
        )

        # get the path to the spec file
        spec_file = pkg_resources.resource_filename(package_name, "specs/amqp0-9-1.xml")
        logging.debug(f"Got spec file: {spec_file}")

        # Load the spec file
        logging.debug("Loading spec file")
        spec = txamqp.spec.load(spec_file)

        # Create a reactor client
        logging.debug("Creating client")
        client = ClientCreator(
            reactor,
            AMQClient,
            delegate=TwistedDelegate(),
            vhost=vhost,
            spec=spec,
            heartbeat=heartbeat,
        )

        # Connect to RabbitMQ
        logging.debug("Connecting to RabbitMQ")
        conn = client.connectTCP(host, port)

        # Add authentication
        logging.debug("Adding authentication callback")
        conn.addCallback(self.gotConnection, username, password)

        # Catch errors
        logging.debug("Adding error callback")
        conn.addErrback(self.ConError)


def console_entry_point():
    parser = argparse.ArgumentParser(
        description=f"Jasmin MongoDB Logger, Log Jasmin SMS Gateway MT/MO to MongoDB Cluster (can be one node).",
        epilog=f"Jasmin SMS Gateway MongoDB Logger v{package_version} - Made with <3 by @8lack0rder - github.com/BlackOrder/jasmin-mongo-logger",
    )

    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version=f"%(prog)s {package_version}",
    )

    parser.add_argument(
        "--amqp-host",
        type=str,
        dest="amqp_broker_host",
        required=False,
        default=DEFAULT_AMQP_BROKER_HOST,
        help=f"AMQP Broker Host (default:{DEFAULT_AMQP_BROKER_HOST})",
    )

    parser.add_argument(
        "--amqp-port",
        type=int,
        dest="amqp_broker_port",
        required=False,
        default=DEFAULT_AMQP_BROKER_PORT,
        help=f"AMQP Broker Port (default:{DEFAULT_AMQP_BROKER_PORT})",
    )

    parser.add_argument(
        "--amqp-vhost",
        type=str,
        dest="amqp_broker_vhost",
        required=False,
        default=DEFAULT_AMQP_BROKER_VHOST,
        help=f"AMQP Broker VHost (default:{DEFAULT_AMQP_BROKER_VHOST})",
    )

    parser.add_argument(
        "--amqp-username",
        type=str,
        dest="amqp_broker_username",
        required=False,
        default=DEFAULT_AMQP_BROKER_USERNAME,
        help=f"AMQP Broker Username (default:{DEFAULT_AMQP_BROKER_USERNAME})",
    )

    parser.add_argument(
        "--amqp-password",
        type=str,
        dest="amqp_broker_password",
        required=False,
        default=DEFAULT_AMQP_BROKER_PASSWORD,
        help=f"AMQP Broker Password (default:{DEFAULT_AMQP_BROKER_PASSWORD})",
    )

    parser.add_argument(
        "--amqp-heartbeat",
        type=int,
        dest="amqp_broker_heartbeat",
        required=False,
        default=DEFAULT_AMQP_BROKER_HEARTBEAT,
        help=f"AMQP Broker Heartbeat (default:{DEFAULT_AMQP_BROKER_HEARTBEAT})",
    )

    parser.add_argument(
        "--retry-on-connection-error",
        dest="retry_on_connection_error",
        required=False,
        default=DEFAULT_RETRY_ON_CONNECTION_ERROR,
        action=argparse.BooleanOptionalAction,
        help=f"Retry on connection error (default:{DEFAULT_RETRY_ON_CONNECTION_ERROR})",
    )

    parser.add_argument(
        "--max-retries",
        type=int,
        dest="max_retries",
        required=False,
        default=DEFAULT_MAX_RETRIES,
        help=f"Max retries (default:{DEFAULT_MAX_RETRIES}) - 0 or any negative integer means retry forever",
    )

    parser.add_argument(
        "--retry-delay",
        type=int,
        dest="retry_delay",
        required=False,
        default=DEFAULT_RETRY_DELAY,
        help=f"Retry delay seconds (default:{DEFAULT_RETRY_DELAY}s)",
    )

    parser.add_argument(
        "--connection-string",
        type=str,
        dest="mongo_connection_string",
        required=os.getenv("MONGO_CONNECTION_STRING") is None,
        default=os.getenv("MONGO_CONNECTION_STRING"),
        help=f"MongoDB Connection String (Default: ** Required **)",
    )

    parser.add_argument(
        "--db",
        type=str,
        dest="logger_database",
        required=os.getenv("MONGO_LOGGER_DATABASE") is None,
        default=os.getenv("MONGO_LOGGER_DATABASE"),
        help=f"MongoDB Logs Database (Default: ** Required **)",
    )

    parser.add_argument(
        "--privacy",
        dest="logger_privacy",
        required=False,
        default=DEFUALT_LOGGER_PRIVACY,
        action=argparse.BooleanOptionalAction,
        help=f"Enable SMS Privacy (default:{DEFUALT_LOGGER_PRIVACY})",
    )

    parser.add_argument(
        "--collection",
        type=str,
        dest="logger_collection",
        required=os.getenv("MONGO_LOGGER_COLLECTION") is None,
        default=os.getenv("MONGO_LOGGER_COLLECTION"),
        help=f"MongoDB Logs Collection (Default: ** Required **)",
    )

    parser.add_argument(
        "--log-level",
        type=str,
        dest="log_level",
        required=False,
        default=DEFAULT_LOG_LEVEL,
        help=f"Log Level (default:{DEFAULT_LOG_LEVEL})",
    )

    parser.add_argument(
        "--log-path",
        type=str,
        dest="log_path",
        required=False,
        default=DEFAULT_LOG_PATH,
        help=f"Log Path (default:{DEFAULT_LOG_PATH})",
    )

    parser.add_argument(
        "--log-file",
        type=str,
        dest="log_file",
        required=False,
        default=DEFAULT_LOG_FILE,
        help=f"Log File (default:{DEFAULT_LOG_FILE})",
    )

    parser.add_argument(
        "--log-rotate",
        type=str,
        dest="log_rotate",
        required=False,
        default=DEFAULT_LOG_ROTATE,
        help=f"Log Rotate (default:{DEFAULT_LOG_ROTATE})",
    )

    parser.add_argument(
        "--file-logging",
        dest="file_logging",
        required=False,
        default=DEFAULT_FILE_LOGGING,
        action=argparse.BooleanOptionalAction,
        help=f"Enable File Logging (default:{DEFAULT_FILE_LOGGING})",
    )

    parser.add_argument(
        "--console-logging",
        dest="console_logging",
        required=False,
        default=DEFAULT_CONSOLE_LOGGING,
        action=argparse.BooleanOptionalAction,
        help=f"Enable Console Logging (default:{DEFAULT_CONSOLE_LOGGING})",
    )

    args = parser.parse_args()

    logReactor = LogReactor(**vars(args))
    logReactor.startReactor()
