import sys
import uuid
import copy
import os
import json
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka import Producer, KafkaError, TopicPartition
from confluent_kafka import avro
from avro import schema
from confluent_kafka.avro import AvroProducer


class KafkaProducer(object):

    def __init__(self):
        self.producers = {}

    def load_schema(self, data):
        if type(data) == schema.RecordSchema:
            data = data
        elif os.path.exists(data):
            data = avro.load(data)
        elif type(data) is str:
            data = str(data)
            data = avro.loads(data)
        return data

    def create_producer(
        self,
        server='127.0.0.1',
        port='9092',
        client_id='Robot',
        group_id=None,
        schema_registry_url=None,
        value_schema=None,
        key_schema=None,
        **kwargs
    ):
        """Create Kafka Producer and returns its `group_id` as string.
        If `schema_registry_url` is used, Kafka Producer client which does avro schema
        encoding to messages is created instead.

        Keyword Arguments:
        - ``server``: (str): IP address / domain, that the consumer should
            contact to bootstrap initial cluster metadata.
            Default: `127.0.0.1`.
        - ``port`` (int): Port number. Default: `9092`.
        - ``client_id`` (str): a name for this client. This string is passed in
            each request to servers and can be used to identify specific
            server-side log entries that correspond to this client. Also
            submitted to GroupCoordinator for logging with respect to
            consumer group administration. Default: `Robot`.
        - ``group_id`` (str or uuid.uuid4() if not set) : name of the consumer group to join for dynamic
            partition assignment (if enabled), and to use for fetching and
            committing offsets. If None, unique string is generated  (via uuid.uuid4())
            and offset commits are disabled. Default: `None`.
        - ``schema_registry_url`` (str): *required* for Avro Consumer. Full URL to avro schema endpoint.
        - ``value_schema`` (str): Optional default avro schema for value or path to file with schema. Default: `None`
        - ``key_schema`` (str): Optional default avro schema for key. Default: `None`

        """
        if group_id is None:
            group_id = str(uuid.uuid4())

        if schema_registry_url:
            if value_schema:
                value_schema = self.load_schema(value_schema)
            if key_schema:
                key_schema = self.load_schema(key_schema)

            producer = AvroProducer({
                'bootstrap.servers': '{}:{}'.format(server, port),
                'group.id': group_id,
                'schema.registry.url': schema_registry_url,
                **kwargs},
                default_key_schema=key_schema,
                default_value_schema=value_schema
            )
        else:
            producer = Producer({
                'bootstrap.servers': '{}:{}'.format(server, port),
                'group.id': group_id,
                **kwargs})

        self.producers[group_id] = producer
        return group_id

    def produce(
        self,
        group_id,
        topic,
        value=None,
        key=None,
        **kwargs
    ):
        """Produce message to topic asynchronously to Kafka by encoding with specified or default avro schema.\n
        https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Producer.produce

        - ``topic`` (str) : name of the topic where to produce message.
        - ``value`` (str|bytes): Message payload.
        - ``key`` (str|bytes): Message key. Default: `None`.
        - ``partition`` (int): Partition to produce to, else uses the configured built-in partitioner.
        """
        self.producers[group_id].produce(
            topic=topic,
            value=value,
            key=key,
            **kwargs
        )

    def flush(self, group_id, timeout=0.1):
        """Wait for all messages in the Producer queue to be delivered. Returns the number of messages still in queue.
        This is a convenience method that calls poll() until len() is zero or the optional timeout elapses.
        - `timeout` (real) : Optional timeout. Default: `0.1`.
        """
        messages_in_queue = self.producers[group_id].flush(timeout)
        return messages_in_queue
