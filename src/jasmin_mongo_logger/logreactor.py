import binascii
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
from smpp.pdu.pdu_types import DataCoding
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.internet.protocol import ClientCreator
from txamqp.client import TwistedDelegate
from txamqp.protocol import AMQClient

from .mongodb import MongoDB

# get the package name this script is running from
package_name = __name__.split(".")[0]

NODEFAULT: str = "REQUIRED: NO_DEFAULT"
DEFAULT_AMQP_BROKER_HOST: str = os.getenv("AMQP_BROKER_HOST", "127.0.0.1")
DEFAULT_AMQP_BROKER_PORT: int = int(os.getenv("AMQP_BROKER_PORT", "5672"))
DEFAULT_AMQP_BROKER_VHOST: str = os.getenv("AMQP_BROKER_VHOST", "/")
DEFAULT_AMQP_BROKER_USERNAME: str = os.getenv("AMQP_BROKER_USERNAME", "guest")
DEFAULT_AMQP_BROKER_PASSWORD: str = os.getenv("AMQP_BROKER_PASSWORD", "guest")
DEFAULT_AMQP_BROKER_HEARTBEAT: int = int(os.getenv("AMQP_BROKER_HEARTBEAT", "0"))

DEFAULT_LOG_LEVEL: str = os.getenv("JASMIN_MONGO_LOGGER_LOG_LEVEL", "INFO").upper()
DEFAULT_LOG_PATH: str = os.getenv("JASMIN_MONGO_LOGGER_LOG_PATH", "/var/log/jasmin")
DEFAULT_LOG_FILE: str = os.getenv(
    "JASMIN_MONGO_LOGGER_LOG_FILE", "%s.log" % package_name
)
DEFAULT_LOG_ROTATE: str = os.getenv("JASMIN_MONGO_LOGGER_LOG_ROTATE", "midnight")
DEFAULT_LOG_FORMAT: str = os.getenv(
    "JASMIN_MONGO_LOGGER_LOG_FORMAT",
    "%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
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
        amqp_broker_host: str = DEFAULT_AMQP_BROKER_HOST,
        amqp_broker_port: int = DEFAULT_AMQP_BROKER_PORT,
        amqp_broker_vhost: str = DEFAULT_AMQP_BROKER_VHOST,
        amqp_broker_username: str = DEFAULT_AMQP_BROKER_USERNAME,
        amqp_broker_password: str = DEFAULT_AMQP_BROKER_PASSWORD,
        amqp_broker_heartbeat: int = DEFAULT_AMQP_BROKER_HEARTBEAT,
        log_level: str = DEFAULT_LOG_LEVEL,
        log_path: str = DEFAULT_LOG_PATH,
        log_file: str = DEFAULT_LOG_FILE,
        log_rotate: str = DEFAULT_LOG_ROTATE,
        file_logging: bool = DEFAULT_FILE_LOGGING,
        console_logging: bool = DEFAULT_CONSOLE_LOGGING,
    ):
        self.AMQP_BROKER_HOST = amqp_broker_host
        self.AMQP_BROKER_PORT = amqp_broker_port
        self.AMQP_BROKER_VHOST = amqp_broker_vhost
        self.AMQP_BROKER_USERNAME = amqp_broker_username
        self.AMQP_BROKER_PASSWORD = amqp_broker_password
        self.AMQP_BROKER_HEARTBEAT = amqp_broker_heartbeat

        self.MONGO_CONNECTION_STRING = mongo_connection_string
        self.MONGO_LOGGER_DATABASE = logger_database
        self.MONGO_LOGGER_COLLECTION = logger_collection
        self.queue = {}

        # Set up logging
        log_format = os.getenv("JASMIN_MONGO_LOGGER_LOG_FORMAT", DEFAULT_LOG_FORMAT)
        log_date_format = os.getenv(
            "JASMIN_MONGO_LOGGER_LOG_DATE_FORMAT", DEFAULT_LOG_DATE_FORMAT
        )

        # Enable logging if console logging or file logging is enabled
        if console_logging or file_logging:
            logFormatter = logging.Formatter(log_format, datefmt=log_date_format)
            rootLogger = logging.getLogger()
            rootLogger.setLevel(log_level)

            # add the handler to the root logger if enabled
            if console_logging:
                consoleHandler = logging.StreamHandler(sys.stdout)
                consoleHandler.setFormatter(logFormatter)
                rootLogger.addHandler(consoleHandler)
                logging.info("Logging to console")

            # add the handler to the root logger if enabled
            if file_logging:
                if not os.path.exists(log_path):
                    os.makedirs(log_path)

                fileHandler = logging.handlers.TimedRotatingFileHandler(
                    filename="%s/%s" % (log_path.rstrip("/"), log_file.lstrip("/")),
                    when=log_rotate,
                )
                fileHandler.setFormatter(logFormatter)
                rootLogger.addHandler(fileHandler)
                logging.info("Logging to file: %s/%s" % (log_path, log_file))
        # Disable logging if console logging and file logging are disabled
        else:
            logging.disable(logging.CRITICAL)

    def startReactor(self):
        logging.info("*********************************************")
        logging.info("::Jasmin MongoDB Logger::")
        logging.info("")
        logging.info("Starting reactor ...")
        logging.info("*********************************************")
        logging.info(" ")

        try:
            self.rabbitMQConnect()
        except Exception as err:
            logging.critical("Error connecting to RabbitMQ server: ")
            logging.critical(err)
            self.tearDown()

    @inlineCallbacks
    def gotConnection(self, conn, username, password):
        logging.info(f"Connected to broker, authenticating: {username}")

        yield conn.start({"LOGIN": username, "PASSWORD": password})

        logging.info("Authenticated. Ready to receive messages")
        logging.info(" ")

        chan = yield conn.channel(1)
        logging.debug("Channel opened")

        # Needed to clean up the connection
        logging.debug("Cleaning up ...")
        self.conn = conn
        self.chan = chan

        logging.debug("Opening channel")
        yield chan.channel_open()

        logging.debug("Declaring queue")
        yield chan.queue_declare(queue="sms_logger_queue")

        # Bind to submit.sm.* and submit.sm.resp.* routes to track sent messages
        logging.debug("Binding to submit.sm.* and submit.sm.resp.* routes")
        yield chan.queue_bind(
            queue="sms_logger_queue", exchange="messaging", routing_key="submit.sm.*"
        )
        yield chan.queue_bind(
            queue="sms_logger_queue",
            exchange="messaging",
            routing_key="submit.sm.resp.*",
        )
        logging.debug("Binding to dlr_thrower.* route")
        # Bind to dlr_thrower.* to track DLRs
        yield chan.queue_bind(
            queue="sms_logger_queue", exchange="messaging", routing_key="dlr_thrower.*"
        )

        logging.debug("Starting consumer")
        yield chan.basic_consume(
            queue="sms_logger_queue", no_ack=False, consumer_tag="sms_logger"
        )
        logging.debug("Consumer started")
        queue = yield conn.queue("sms_logger")

        logging.debug("Connecting to MongoDB")
        mongosource = MongoDB(
            connection_string=self.MONGO_CONNECTION_STRING,
            database_name=self.MONGO_LOGGER_DATABASE,
        )

        logging.debug("Checking MongoDB connection")
        if mongosource.startConnection() is not True:
            logging.debug("MongoDB connection failed")
            return

        logging.debug("MongoDB connection successful")
        # Wait for messages
        # This can be done through a callback ...
        logging.debug("Starting message processing")

        try:
            logging.debug("Starting Daemon")
            while True:
                logging.debug("Waiting for messages")
                msg = yield queue.get()

                logging.debug("Got message")
                props = msg.content.properties
                logging.debug("Processing message")
                logging.debug(f"Message ID: {props['message-id']}")
                logging.debug(f"Routing key: {msg.routing_key}")
                logging.debug(f"Headers: {props['headers']}")
                logging.debug(f"Payload: {msg.content.body}")
                logging.debug(" ")

                if (
                    msg.routing_key[:10] == "submit.sm."
                    and msg.routing_key[:15] != "submit.sm.resp."
                ):
                    # It's a submit_sm
                    logging.debug("It's a submit_sm")

                    pdu = pickle.loads(msg.content.body)
                    pdu_count = 1
                    short_message = pdu.params["short_message"]
                    billing = props["headers"]
                    billing_pickle = billing.get("submit_sm_resp_bill")
                    if not billing_pickle:
                        billing_pickle = billing.get("submit_sm_bill")
                    submit_sm_bill = pickle.loads(billing_pickle)
                    source_connector = props["headers"]["source_connector"]
                    routed_cid = msg.routing_key[10:]

                    # Is it a multipart message ?
                    while hasattr(pdu, "nextPdu"):
                        # Remove UDH from first part
                        if pdu_count == 1:
                            short_message = short_message[6:]

                        pdu = pdu.nextPdu

                        # Update values:
                        pdu_count += 1
                        short_message += pdu.params["short_message"][6:]

                    # Save short_message bytes
                    binary_message = binascii.hexlify(short_message)

                    # If it's a binary message, assume it's utf_16_be encoded
                    if pdu.params["data_coding"] is not None:
                        dc = pdu.params["data_coding"]
                        if (isinstance(dc, int) and dc == 8) or (
                            isinstance(dc, DataCoding) and str(dc.schemeData) == "UCS2"
                        ):
                            short_message = short_message.decode(
                                "utf_16_be", "ignore"
                            ).encode("utf_8")

                    # Save message in queue
                    logging.debug("Saving message in queue")
                    self.queue[props["message-id"]] = {
                        "source_connector": source_connector,
                        "routed_cid": routed_cid,
                        "rate": submit_sm_bill.getTotalAmounts(),
                        "charge": submit_sm_bill.getTotalAmounts() * pdu_count,
                        "uid": submit_sm_bill.user.uid,
                        "destination_addr": pdu.params["destination_addr"],
                        "source_addr": pdu.params["source_addr"],
                        "pdu_count": pdu_count,
                        "short_message": short_message,
                        "binary_message": binary_message,
                    }

                    # Save message in MongoDB
                    logging.debug("Saving message in MongoDB")
                    mongosource.update_one(
                        module=self.MONGO_LOGGER_COLLECTION,
                        sub_id=props["message-id"],
                        data={
                            "source_connector": source_connector,
                            "routed_cid": routed_cid,
                            "rate": submit_sm_bill.getTotalAmounts(),
                            "charge": submit_sm_bill.getTotalAmounts() * pdu_count,
                            "uid": submit_sm_bill.user.uid,
                            "destination_addr": pdu.params["destination_addr"],
                            "source_addr": pdu.params["source_addr"],
                            "pdu_count": pdu_count,
                            "short_message": short_message,
                            "binary_message": binary_message,
                        },
                    )
                elif msg.routing_key[:15] == "submit.sm.resp.":
                    # It's a submit_sm_resp
                    logging.debug("It's a submit_sm_resp")

                    pdu = pickle.loads(msg.content.body)
                    if props["message-id"] not in self.queue:
                        logging.error(
                            f" Got resp of an unknown submit_sm: {props['message-id']}"
                        )
                        chan.basic_ack(delivery_tag=msg.delivery_tag)
                        continue

                    qmsg = self.queue[props["message-id"]]

                    if qmsg["source_addr"] is None:
                        qmsg["source_addr"] = ""

                    if qmsg["destination_addr"] is None:
                        qmsg["destination_addr"] = ""

                    if qmsg["short_message"] is None:
                        qmsg["short_message"] = ""

                    # Update message status
                    logging.debug("Updating message status in MongoDB")
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
                    if props["message-id"] not in self.queue:
                        logging.error(
                            f" Got dlr of an unknown submit_sm: {props['message-id']}"
                        )
                        chan.basic_ack(delivery_tag=msg.delivery_tag)
                        continue

                    # Update message status
                    logging.debug("Updating message status in MongoDB")
                    qmsg = self.queue[props["message-id"]]

                    mongosource.update_one(
                        module=self.MONGO_LOGGER_COLLECTION,
                        sub_id=props["message-id"],
                        data={
                            "status": props["headers"]["message_status"],
                            "status_at": datetime.now(),
                        },
                    )

                else:
                    logging.error(f" unknown route: {msg.routing_key}")

                chan.basic_ack(delivery_tag=msg.delivery_tag)
                logging.debug("Message processed")
                logging.debug(" ")

        except KeyboardInterrupt:
            logging.critical("KeyboardInterrupt")
        except Exception as err:
            logging.critical("Error: ")
            logging.critical(err)
        except:
            logging.critical("Unknown error")

        self.tearDown()

    def tearDown(self):
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

    def cleanConnectionBreak(self):
        # A clean way to tear down and stop
        logging.debug("Cleaning up connection")
        yield self.chan.basic_cancel("sms_logger")
        logging.debug("Closing channel")
        yield self.chan.channel_close()
        logging.debug("Closing channel 0")
        chan0 = yield self.conn.channel(0)
        logging.debug("Closing connection")
        yield chan0.connection_close()
        logging.debug("Cleaning up done")

    def rabbitMQConnect(self):
        # Connect to RabbitMQ
        logging.debug("Connecting to RabbitMQ")

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
        logging.debug(f"Using spec file: {spec_file}")

        spec = txamqp.spec.load(spec_file)

        def whoops(err):
            logging.critical("Error connecting to RabbitMQ server: ")
            logging.critical(err)
            self.tearDown()

        # Connect and authenticate
        logging.debug("Connecting to broker")
        d = ClientCreator(
            reactor,
            AMQClient,
            delegate=TwistedDelegate(),
            vhost=vhost,
            spec=spec,
            heartbeat=heartbeat,
        ).connectTCP(host, port)

        # Add authentication
        logging.debug("Adding authentication")
        d.addCallback(self.gotConnection, username, password)

        # Catch errors
        d.addErrback(whoops)

        # Run the reactor
        logging.debug("Running reactor")
        reactor.run()


def console_entry_point():
    # get the package name this script is running from
    package_name = __name__.split(".")[0]
    print(__name__)
    parser = argparse.ArgumentParser(
        description=f"Jasmin MongoDB Logger, Log Jasmin SMS Gateway MT/MO to MongoDB Cluster (can be one node).",
        epilog=f"Jasmin SMS Gateway MongoDB Logger v{pkg_resources.get_distribution(package_name).version} - Made with <3 by @8lack0rder - github.com/BlackOrder/jasmin-mongo-logger",
    )

    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version=f"%(prog)s {pkg_resources.get_distribution(package_name).version}",
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
