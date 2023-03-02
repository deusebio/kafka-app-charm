import datetime
import json
import logging
import threading
import time
import uuid
from typing import List, Optional

from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from pymongo.errors import DuplicateKeyError

from lib.charms.kafka.v0.client import KafkaClient, retrying

logger = logging.getLogger(__name__)

class Producer(threading.Thread):
    def __init__(
        self,
        servers: List[str],
        username: str,
        password: str,
        security_protocol: str,
        topic: str,
        num_partitions: int,
        replication_factor: int,
        number_of_messages:int,
        unit_hostname: str,
        private_key: Optional[str] = None,
        producer_collection =None,
    ):
        self.servers = servers
        self.username = username
        self.password = password
        self.private_key = private_key
        self.security_protocol = security_protocol
        self.num_partitions = num_partitions
        self.topic = topic
        self.replication_factor = replication_factor
        self.number_of_messages = number_of_messages
        self.producer_collection = producer_collection
        self.unit_hostname = unit_hostname

        self.exc = None
        threading.Thread.__init__(self)

    def run(self):
        logger.info("starting producer...")
        client = retrying(
        lambda: KafkaClient(
            servers=self.servers,
            username=self.username,
            password=self.password,
            security_protocol=self.security_protocol,
            cafile_path=self.cafile_path,
            certfile_path=self.certfile_path,
            keyfile_path=self.keyfile_path,
        ),
        Exception
        )
        topic = NewTopic(
            name=self.topic,
            num_partitions=self.num_partitions,
            replication_factor=self.replication_factor,
        )
        try:
            retrying(lambda: client.create_topic(topic=topic), AssertionError)
        except TopicAlreadyExistsError:
            logger.info("Topic already exists")
        except Exception as e:
            self.exc = e
            return

        time.sleep(2)

        logger.info("--producer - Starting...")

        for i in range(self.num_messages):
            message = {
                "timestamp": datetime.datetime.now().timestamp(),
                "_id": uuid.uuid4().hex,
                "origin": self.unit_hostname,
                "content": f"Message #{str(i)}"
            }
            if self.producer_collection is not None:
                self.producer_collection.insert_one(message)
            client.produce_message(
                topic_name=self.topic, message_content=json.dumps(message)
            )
            time.sleep(2)

class Consumer(threading.Thread):
    def __init__(
        self,
        servers: List[str],
        username: str,
        password: str,
        security_protocol: str,
        topic: str,
        num_partitions: int,
        replication_factor: int,
        number_of_messages:int,
        consumer_group_prefix: str,
        unit_hostname: str,
        private_key: Optional[str] = None,
        consumer_collection =None,
    ):
        self.servers = servers
        self.username = username
        self.password = password
        self.private_key = private_key
        self.security_protocol = security_protocol
        self.num_partitions = num_partitions
        self.topic = topic
        self.replication_factor = replication_factor
        self.number_of_messages = number_of_messages
        self.consumer_collection = consumer_collection
        self.unit_hostname = unit_hostname
        self.consumer_group_prefix = consumer_group_prefix
        self.exc = None

        threading.Thread.__init__(self)

    def run(self):
        logger.info("--consumer - Starting...")
        logger.info("starting producer...")

        client = retrying(
        lambda: KafkaClient(
            servers=self.servers,
            username=self.username,
            password=self.password,
            security_protocol=self.security_protocol,
            cafile_path=self.cafile_path,
            certfile_path=self.certfile_path,
            keyfile_path=self.keyfile_path,
        ),
        Exception
        )
        client.subscribe_to_topic(
            topic_name=self.topic, consumer_group_prefix=self.consumer_group_prefix
        )
        for message in client.messages():
            logger.info(message)
            content = json.loads(message.value.decode("utf-8"))
            content["timestamp"] = datetime.datetime.now().timestamp()
            content["destination"] = self.unit_hostname
            content["consumer_group"] = self.consumer_group_prefix
            if self.consumer_collection is not None:
                try:
                    self.consumer_collection.insert_one(content)
                except DuplicateKeyError as e:
                    logger.error(f"Duplicated key with id: {content['_id']}")
                    self.exc = e

    def join(self, timeout=None):
        super(Consumer, self).join(timeout)
        if self.exc:
            raise self.exc
