import binascii
import logging
import os
import pickle as pickle
from datetime import datetime
from time import sleep

import pkg_resources
import txamqp.spec
from smpp.pdu.pdu_types import DataCoding
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.internet.protocol import ClientCreator
from twisted.python import log
from txamqp.client import TwistedDelegate
from txamqp.protocol import AMQClient

from .mongodb import MongoDB

NODEFAULT: str = "REQUIRED: NO_DEFAULT"
DEFAULT_AMQP_BROKER_HOST: str = "127.0.0.1"
DEFAULT_AMQP_BROKER_PORT: int = 5672
DEFAULT_LOG_PATH: str = "/var/log/jasmin/"
DEFAULT_LOG_LEVEL: str = "INFO"


class LogReactor:
    def __init__(
        self, mongo_connection_string: str,
        logger_database: str,
        logger_collection: str,
        amqp_broker_host: str = DEFAULT_AMQP_BROKER_HOST,
        amqp_broker_port: int = DEFAULT_AMQP_BROKER_PORT,
        log_path: str = DEFAULT_LOG_PATH,
        log_level: str = DEFAULT_LOG_LEVEL
    ):
        self.AMQP_BROKER_HOST = amqp_broker_host
        self.AMQP_BROKER_PORT = amqp_broker_port
        self.MONGO_CONNECTION_STRING = mongo_connection_string
        self.MONGO_LOGGER_DATABASE = logger_database
        self.MONGO_LOGGER_COLLECTION = logger_collection
        self.queue = {}

        logFormatter = logging.Formatter(
            "%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
        rootLogger = logging.getLogger()
        rootLogger.setLevel(log_level)

        consoleHandler = logging.StreamHandler()
        consoleHandler.setLevel(log_level)
        consoleHandler.setFormatter(logFormatter)
        rootLogger.addHandler(consoleHandler)

        if not os.path.exists(log_path):
            os.makedirs(log_path)

        fileHandler = logging.FileHandler(
            f"{log_path.rstrip('/')}/jasmin_mongo_logger.log")
        fileHandler.setLevel(log_level)
        fileHandler.setFormatter(logFormatter)
        rootLogger.addHandler(fileHandler)

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

        # Needed to clean up the connection
        self.conn = conn
        self.chan = chan

        yield chan.channel_open()

        yield chan.queue_declare(queue="sms_logger_queue")

        # Bind to submit.sm.* and submit.sm.resp.* routes to track sent messages
        yield chan.queue_bind(queue="sms_logger_queue", exchange="messaging", routing_key='submit.sm.*')
        yield chan.queue_bind(queue="sms_logger_queue", exchange="messaging", routing_key='submit.sm.resp.*')
        # Bind to dlr_thrower.* to track DLRs
        yield chan.queue_bind(queue="sms_logger_queue", exchange="messaging", routing_key='dlr_thrower.*')

        yield chan.basic_consume(queue='sms_logger_queue', no_ack=False, consumer_tag="sms_logger")
        queue = yield conn.queue("sms_logger")

        mongosource = MongoDB(connection_string=self.MONGO_CONNECTION_STRING,
                              database_name=self.MONGO_LOGGER_DATABASE)

        if mongosource.startConnection() is not True:
            return

        # Wait for messages
        # This can be done through a callback ...
        while True:
            msg = yield queue.get()
            props = msg.content.properties

            if msg.routing_key[:10] == 'submit.sm.' and msg.routing_key[:15] != 'submit.sm.resp.':
                pdu = pickle.loads(msg.content.body)
                pdu_count = 1
                short_message = pdu.params['short_message']
                billing = props['headers']
                billing_pickle = billing.get('submit_sm_resp_bill')
                if not billing_pickle:
                    billing_pickle = billing.get('submit_sm_bill')
                submit_sm_bill = pickle.loads(billing_pickle)
                source_connector = props['headers']['source_connector']
                routed_cid = msg.routing_key[10:]

                # Is it a multipart message ?
                while hasattr(pdu, 'nextPdu'):
                    # Remove UDH from first part
                    if pdu_count == 1:
                        short_message = short_message[6:]

                    pdu = pdu.nextPdu

                    # Update values:
                    pdu_count += 1
                    short_message += pdu.params['short_message'][6:]

                # Save short_message bytes
                binary_message = binascii.hexlify(short_message)

                # If it's a binary message, assume it's utf_16_be encoded
                if pdu.params['data_coding'] is not None:
                    dc = pdu.params['data_coding']
                    if (isinstance(dc, int) and dc == 8) or (isinstance(dc, DataCoding) and str(dc.schemeData) == 'UCS2'):
                        short_message = short_message.decode(
                            'utf_16_be', 'ignore').encode('utf_8')

                self.queue[props['message-id']] = {
                    'source_connector': source_connector,
                    'routed_cid': routed_cid,
                    'rate': submit_sm_bill.getTotalAmounts(),
                    'charge': submit_sm_bill.getTotalAmounts() * pdu_count,
                    'uid': submit_sm_bill.user.uid,
                    'destination_addr': pdu.params['destination_addr'],
                    'source_addr': pdu.params['source_addr'],
                    'pdu_count': pdu_count,
                    'short_message': short_message,
                    'binary_message': binary_message,
                }

                mongosource.update_one(
                    module=self.MONGO_LOGGER_COLLECTION,
                    sub_id=props['message-id'],
                    data={
                        "source_connector": source_connector,
                        "routed_cid": routed_cid,
                        "rate": submit_sm_bill.getTotalAmounts(),
                        "charge": submit_sm_bill.getTotalAmounts() * pdu_count,
                        "uid": submit_sm_bill.user.uid,
                        "destination_addr": pdu.params['destination_addr'],
                        "source_addr": pdu.params['source_addr'],
                        "pdu_count": pdu_count,
                        "short_message": short_message,
                        "binary_message": binary_message
                    }
                )
            elif msg.routing_key[:15] == 'submit.sm.resp.':
                # It's a submit_sm_resp

                pdu = pickle.loads(msg.content.body)
                if props['message-id'] not in self.queue:
                    logging.error(
                        f" Got resp of an unknown submit_sm: {props['message-id']}")
                    chan.basic_ack(delivery_tag=msg.delivery_tag)
                    continue

                qmsg = self.queue[props['message-id']]

                if qmsg['source_addr'] is None:
                    qmsg['source_addr'] = ''

                mongosource.update_one(
                    module=self.MONGO_LOGGER_COLLECTION,
                    sub_id=props['message-id'],
                    data={
                        "source_addr": qmsg['source_addr'],
                        "rate": qmsg['rate'],
                        "pdu_count": qmsg['pdu_count'],
                        "charge": qmsg['charge'],
                        "destination_addr": qmsg['destination_addr'],
                        "short_message": qmsg['short_message'],
                        "status": pdu.status,
                        "uid": qmsg['uid'],
                        "created_at": props['headers']['created_at'],
                        "binary_message": qmsg['binary_message'],
                        "routed_cid": qmsg['routed_cid'],
                        "source_connector": qmsg['source_connector'],
                        "status_at": props['headers']['created_at']
                    }
                )

            elif msg.routing_key[:12] == 'dlr_thrower.':
                if props['headers']['message_status'][:5] == 'ESME_':
                    # Ignore dlr from submit_sm_resp
                    chan.basic_ack(delivery_tag=msg.delivery_tag)
                    continue

                # It's a dlr
                if props['message-id'] not in self.queue:
                    logging.error(
                        f" Got dlr of an unknown submit_sm: {props['message-id']}")
                    chan.basic_ack(delivery_tag=msg.delivery_tag)
                    continue

                # Update message status
                qmsg = self.queue[props['message-id']]

                mongosource.update_one(
                    module=self.MONGO_LOGGER_COLLECTION,
                    sub_id=props['message-id'],
                    data={
                        "status": props['headers']['message_status'],
                        "status_at": datetime.now()
                    }
                )

            else:
                logging.error(f" unknown route: {msg.routing_key}")

            chan.basic_ack(delivery_tag=msg.delivery_tag)

        self.tearDown()

    def tearDown(self):
        logging.critical("Shutting down !!!")
        logging.critical("Cleaning up ...")

        self.cleanConnectionBreak()

        if reactor.running:
            reactor.stop()
        sleep(3)

    def cleanConnectionBreak(self):
        # A clean way to tear down and stop
        yield self.chan.basic_cancel("sms_logger")
        yield self.chan.channel_close()
        chan0 = yield self.conn.channel(0)
        yield chan0.connection_close()

    def rabbitMQConnect(self):
        host = self.AMQP_BROKER_HOST
        port = self.AMQP_BROKER_PORT
        vhost = '/'
        username = 'guest'
        password = 'guest'
        spec_file = pkg_resources.resource_filename(
            'jasmin_mongo_logger', 'amqp0-9-1.xml')

        spec = txamqp.spec.load(spec_file)

        def whoops(err):
            logging.critical("Error in RabbitMQ server: ")
            logging.critical(err)
            self.tearDown()

        # Connect and authenticate
        d = ClientCreator(reactor,
                          AMQClient,
                          delegate=TwistedDelegate(),
                          vhost=vhost,
                          spec=spec,
                          heartbeat=5).connectTCP(host, port)
        d.addCallback(self.gotConnection, username, password)

        d.addErrback(whoops)
        reactor.run()


def startFromCLI():
    import argparse
    parser = argparse.ArgumentParser(
        description=f"Jasmin MongoDB Logger, Log Jasmin SMS Gateway MT/MO to MongoDB Cluster (can be one node).")

    parser.add_argument('-v', '--version', action='version',
                        version=f'%(prog)s {pkg_resources.get_distribution("jasmin_mongo_logger").version}')

    parser.add_argument('--amqp_host', type=str,
                        dest='amqp_broker_host',
                        required=False,
                        default=os.getenv("AMQP_BROKER_HOST",
                                          DEFAULT_AMQP_BROKER_HOST),
                        help=f'AMQP Broker Host (default:{DEFAULT_AMQP_BROKER_HOST})')

    parser.add_argument('--amqp_port', type=int,
                        dest='amqp_broker_port',
                        required=False,
                        default=os.getenv("AMQP_BROKER_PORT",
                                          DEFAULT_AMQP_BROKER_PORT),
                        help=f'AMQP Broker Port (default:{DEFAULT_AMQP_BROKER_PORT})')

    parser.add_argument('--connection_string', type=str,
                        dest='mongo_connection_string',
                        required=os.getenv(
                            "MONGO_CONNECTION_STRING") is None,
                        default=os.getenv("MONGO_CONNECTION_STRING"),
                        help=f'MongoDB Connection String (Default: ** Required **)')

    parser.add_argument('--db', type=str,
                        dest='logger_database',
                        required=os.getenv("MONGO_LOGGER_DATABASE") is None,
                        default=os.getenv("MONGO_LOGGER_DATABASE"),
                        help=f'MongoDB Logs Database (Default: ** Required **)')

    parser.add_argument('--collection', type=str,
                        dest='logger_collection',
                        required=os.getenv(
                            "MONGO_LOGGER_COLLECTION") is None,
                        default=os.getenv("MONGO_LOGGER_COLLECTION"),
                        help=f'MongoDB Logs Collection (Default: ** Required **)')

    parser.add_argument('--log_path', type=str,
                        dest='log_path',
                        required=False,
                        default=os.getenv("JASMIN_MONGO_LOGGER_LOG_PATH",
                                          DEFAULT_LOG_PATH),
                        help=f'Log Path (default:{DEFAULT_LOG_PATH})')

    parser.add_argument('--log_level', type=str,
                        dest='log_level',
                        required=False,
                        default=os.getenv("JASMIN_MONGO_LOGGER_LOG_LEVEL",
                                          DEFAULT_LOG_LEVEL),
                        help=f'Log Level (default:{DEFAULT_LOG_LEVEL})')

    args = parser.parse_args()

    logReactor = LogReactor(**vars(args))
    logReactor.startReactor()
