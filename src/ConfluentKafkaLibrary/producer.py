import uuid
import os
from avro import schema
from confluent_kafka import SerializingProducer
from confluent_kafka import Producer
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer


class KafkaProducer():

    def __init__(self):
        self.producers = {}

    def load_schema(self, data):
        if os.path.exists(data):
            data = avro.load(data)
        elif isinstance(data,str):
            data = str(data)
            data = avro.loads(data)
        if not isinstance(data, schema.RecordSchema):
            raise Exception("Data is stil not in schema.RecordSchema format, data: " + data)
        return data

    def create_producer(
        self,
        server='127.0.0.1',
        port='9092',
        group_id=None,
        schema_registry_url=None,
        value_schema=None,
        key_schema=None,
        key_serializer=None,
        value_serializer=None,
        legacy=True,
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
        - ``schema_registry_url`` (str): *required* for Avro Consumer. Full URL to avro schema endpoint.
        - ``value_schema`` (str): Optional default avro schema for value or path to file with schema. Default: `None`
        - ``key_schema`` (str): Optional default avro schema for key. Default: `None`
        - ``legacy`` (bool): Activate SerializingProducer if 'False' else
            AvroProducer (legacy) is used. Will be removed when confluent-kafka will deprecate this.
            Default: `True`.

        """
        if group_id is None:
            group_id = str(uuid.uuid4())

        if schema_registry_url and legacy:
            if value_schema:
                value_schema = self.load_schema(value_schema)
            if key_schema:
                key_schema = self.load_schema(key_schema)

            producer = AvroProducer({
                'bootstrap.servers': '{}:{}'.format(server, port),
                'schema.registry.url': schema_registry_url,
                **kwargs},
                default_key_schema=key_schema,
                default_value_schema=value_schema
            )
        elif not legacy:
            producer = SerializingProducer({
                'bootstrap.servers': '{}:{}'.format(server, port),
                'key.serializer': key_serializer,
                'value.serializer': value_serializer,
                **kwargs}
            )
        else:
            producer = Producer({
                'bootstrap.servers': '{}:{}'.format(server, port),
                **kwargs})

        self.producers[group_id] = producer
        return group_id

    def produce(
        self,
        group_id,
        topic,
        value=None,
        key=None,
        headers=None,
        **kwargs
    ):
        """Produce message to topic asynchronously to Kafka by encoding with specified or default avro schema.\n
        https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Producer.produce

        - ``topic`` (str) : name of the topic where to produce message.
        - ``value`` (str|bytes): Message payload.
        - ``key`` (str|bytes): Message key. Default: `None`.
        - ``headers`` (dict[str, bytes]): Message headers. Default: `None`.
        - ``partition`` (int): Partition to produce to, else uses the configured built-in partitioner.
        """
        self.producers[group_id].produce(
            topic=topic,
            value=value,
            key=key,
            headers=headers,
            **kwargs
        )

    def flush(self, group_id, timeout=0.1):
        """Wait for all messages in the Producer queue to be delivered. Returns the number of messages still in queue.
        This is a convenience method that calls poll() until len() is zero or the optional timeout elapses.
        - `timeout` (real) : Optional timeout. Default: `0.1`.
        """
        messages_in_queue = self.producers[group_id].flush(timeout)
        return messages_in_queue

    def purge(self, group_id, **kwargs):
        """Purge messages currently handled by the producer instance.
        """
        self.producers[group_id].purge(**kwargs)
