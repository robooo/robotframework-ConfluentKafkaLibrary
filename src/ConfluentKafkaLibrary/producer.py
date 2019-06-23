import sys
import uuid
import copy
import json
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka import Producer, KafkaError, TopicPartition
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer


class KafkaProducer(object):

    def __init__(self):
        self.__producers = {}

    def create_producer(
        self,
        server='127.0.0.1',
        port='9092',
        client_id='Robot',
        group_id=None,
        schema_registry_url=None,
        value_schema_str=None,
        key_schema_str=None,
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
        - ``value_schema_str`` (str): Optional default avro schema for value. Default: `None`
        - ``key_schema_str`` (str): Optional default avro schema for key. Default: `None`

        """
        if group_id is None:
            group_id = str(uuid.uuid4())

        if schema_registry_url:
            value_schema = avro.loads(value_schema_str)
            key_schema = avro.loads(key_schema_str)

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

        self.__producers[group_id] = producer
        return group_id

    def produce(
        self,
        group_id,
        topic,
        value,
        key=None,
        value_encoding='utf-8',
        callback_func=None
    ):
        """Produce message to topic asynchronously to Kafka by encoding with specified or default avro schema.

        - ``topic` (str) : name of the topic where to produce message.
        - ``data` (str) : Message which produce to topic.
        - ``data_encoding` (str) : encode to specific format. Default: `utf-8`.
        - ``callback_func` (func) : the function that will be called from poll()
        - ``key`` (object) : An object to serialize. Default: `None`.
            when the message has been successfully delivered or permanently fails delivery..
        """
        self.__producers[group_id].produce(
            topic=topic,
            value=value.encode(value_encoding),
            callback=callback_func,
            key=key
        )

    def flush(self, group_id, timeout=1.0):
        """Wait for all messages in the Producer queue to be delivered.
        This is a convenience method that calls poll() until len() is zero or the optional timeout elapses.
        - `timeout` (real) : Optional timeout. Default: `1.0`.
        """
        self.__producers[group_id].flush(timeout)

    def list_topics(self, group_id,):
        return self.__producers[group_id].list_topics()
