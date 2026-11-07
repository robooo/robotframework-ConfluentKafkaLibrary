import uuid
from confluent_kafka import SerializingProducer
from confluent_kafka import Producer

class KafkaProducer():

    def __init__(self):
        self.producers = {}

    def create_producer(
        self,
        server='127.0.0.1',
        port='9092',
        group_id=None,
        key_serializer=None,
        value_serializer=None,
        serializing=False,
        **kwargs
    ):
        """Create Kafka Producer and returns its `group_id` as string.

        Keyword Arguments:
        - ``server``: (str): IP address / domain, that the consumer should
            contact to bootstrap initial cluster metadata.
            Default: `127.0.0.1`.
        - ``port`` (int): Port number. Default: `9092`.
        - ``serializing`` (bool): Activate SerializingProducer with serialization capabilities.
            Default: `False`
        """
        if group_id is None:
            group_id = str(uuid.uuid4())

        if serializing:
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

    def close_producer(self, group_id, timeout=None):
        """Flush outstanding messages, close down and terminate the Kafka Producer.
        Returns the number of messages which were not delivered and had to be purged.

        - ``group_id`` (str): id of the producer returned by `Create Producer`.
        - ``timeout`` (float): Maximum time in seconds to wait for delivery of outstanding
            messages. Messages still undelivered after the timeout are purged (dropped).
            If not set, waits until all messages are delivered or failed, which can take
            up to ``message.timeout.ms`` (librdkafka default: 5 minutes) when Kafka is down.
            Default: `None`.
        """
        producer = self.producers.pop(group_id, None)
        if producer is None:
            raise ValueError(f"Producer with group_id '{group_id}' does not exist!")

        purged = 0
        if timeout is not None:
            purged = producer.flush(float(timeout))
            if purged:
                producer.purge()
        producer.close()
        return purged
