import sys
import uuid
import copy
import json
import threading
from threading import Thread, Timer
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka import Consumer, KafkaError, TopicPartition
from confluent_kafka.avro import AvroConsumer


class GetMessagesThread(Thread):

    def __init__(
        self,
        server='127.0.0.1',
        port='9092',
        topics='',
        group_id='',
        **kwargs
    ):

        super(GetMessagesThread, self).__init__()
        self.daemon = True
        self.consumer = KafkaConsumer()
        self.group_id = self.consumer.create_consumer(server=server, port=port, group_id=group_id, **kwargs)
        if not isinstance(topics, list):
            topics = [topics]
        self.consumer.subscribe_topic(self.group_id, topics=topics)
        self.messages = self.consumer.poll(group_id=self.group_id)

    def run(self):
        threading.Timer(1, self.run).start()
        self.messages += self.consumer.poll(group_id=self.group_id)

    def stop(self):
        self._is_running = False


class KafkaConsumer(object):

    ROBOT_LIBRARY_SCOPE = 'GLOBAL'

    def __init__(self):
        self.__consumers = {}

    def create_consumer(
        self,
        server="127.0.0.1",
        port="9092",
        client_id='Robot',
        group_id=str(uuid.uuid4()),
        enable_auto_commit=True,
        auto_offset_reset="latest",
        schema_registry_url=None,
        **kwargs
    ):
        """Create Kafka Consumer and returns its `group_id` as string.

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
        - ``auto_offset_reset`` (str): A policy for resetting offsets on
            OffsetOutOfRange errors: `earliest` will move to the oldest
            available message, `latest` will move to the most recent. Any
            other value will raise the exception. Default: `latest`.
        - ``enable_auto_commit`` (bool): If true the consumer's offset will be
            periodically committed in the background. Default: `True`.
        - ``schema_registry_url`` (str): *required* for Avro Consumer. Full URL to avro schema endpoint.

        Note:
        Configuration parameters are described in more detail at
        https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md \n
        """

        if schema_registry_url:
            consumer = AvroConsumer({
                'bootstrap.servers': '{}:{}'.format(server, port),
                'group.id': group_id,
                'enable.auto.commit': enable_auto_commit,
                'default.topic.config': {
                    'auto.offset.reset': auto_offset_reset
                },
                'schema.registry.url': schema_registry_url,
                **kwargs})
        else:
            consumer = Consumer({
                'bootstrap.servers': '{}:{}'.format(server, port),
                'group.id': group_id,
                'enable.auto.commit': enable_auto_commit,
                'default.topic.config': {
                    'auto.offset.reset': auto_offset_reset
                },
                **kwargs})

        self.__consumers[group_id] = consumer
        return group_id

    def _is_assigned(self, group_id, topic_partition):
        for tp in topic_partition:
            if tp in self.__consumers[group_id].assignment():
                return True
        return False

    def assign_to_topic_partition(self, group_id, topic_partition):
        """Assign a list of TopicPartitions.

        - ``partitions`` (list of `TopicPartition`): Assignment for this instance.
        """

        if isinstance(topic_partition, TopicPartition):
            topic_partition = [topic_partition]
        if not self._is_assigned(group_id, topic_partition):
            self.__consumers[group_id].assign(topic_partition)

    def subscribe_topic(self, group_id, topics):
        """Subscribe to a list of topics, or a topic regex pattern.

        - ``topics`` (list): List of topics for subscription.
        """

        if not isinstance(topics, list):
            topics = [topics]
        self.__consumers[group_id].subscribe(topics)

    def unsubscribe(self, group_id):
        """Unsubscribe of topics.
        """
        self.__consumers[group_id].unsubscribe()

    def close_consumer(self, group_id):
        """Close down and terminate the Kafka Consumer.
        """
        self.__consumers[group_id].close()

    def poll(
        self,
        group_id,
        timeout=1,
        max_records=1,
        poll_attempts=10
    ):
        """Fetch and return messages from assigned topics / partitions as list.
        - ``max_records`` (int): maximum number of messages to get from poll. Default: 1.
        - ``timeout`` (int): Seconds spent waiting in poll if data is not available in the buffer.
        If 0, returns immediately with any records that are available currently in the buffer, else returns empty.
        Must not be negative. Default: `1`
        -  ``poll_attempts`` (int): Attempts to consume messages and endless looping prevention.
        Sometimes the first messages are None or the topic could be empty. Default: `10`.
        """

        messages = []
        while poll_attempts > 0:
            try:
                msg = self.__consumers[group_id].poll(timeout=timeout)
            except SerializerError as e:
                print('Message deserialization failed for {}: {}'.format(msg, e))
                break

            if msg is None:
                poll_attempts -= 1
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    poll_attempts = 0
                    continue
                else:
                    print(msg.error())
                    break

            messages.append(msg.value())

            if len(messages) == max_records:
                return messages

        return messages

    # Experimental keywords
    def decode_data(self, data, decode_format, remove_zero_bytes):
        """ TODO
        """
        if decode_format and remove_zero_bytes:
            return [record.decode(str(decode_format)).replace('\x00', '') for record in data]
        elif decode_format and not remove_zero_bytes:
            return [record.decode(str(format)) for record in data]
        elif not decode_format and remove_zero_bytes:
            return [record.replace('\x00', '') for record in data]
        else:
            return data

    # Experimental - getting messages from kafka topic every second
    def start_consumer_threaded(
        self,
        topics,
        group_id=str(uuid.uuid4()),
        server='127.0.0.1',
        port='9092',
        **kwargs
    ):
        if topics is None:
            raise ValueError("Topics can not be empty!")

        t = GetMessagesThread(server, port, topics, group_id=group_id, **kwargs)
        t.start()
        t.join()
        return t

    def get_messages_from_thread(self, running_thread, decode_format=None, remove_zero_bytes=False):
        """ Returns all records gathered from specific thread
            - ``running_thread`` (Thread object) - thread which was executed by `Start Consumer Threaded`
            - ``decode_data`` (str) - If you need to decode data to specific format
                (See https://docs.python.org/3/library/codecs.html#standard-encodings). Default: None.
            - ``remove_zero_bytes`` (bool) - When you are working with byte streams
                you can end up with a lot of '\\x00' bytes you want to remove. Default: False.
        """
        records = self.decode_data(
            data=running_thread.messages,
            decode_format=decode_format,
            remove_zero_bytes=remove_zero_bytes
        )
        return records

    def stop_thread(self, running_thread):
        running_thread.stop()
