import uuid
from threading import Thread
from confluent_kafka import Consumer, KafkaError, TopicPartition
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka.avro import AvroConsumer


class GetMessagesThread(Thread):

    def __init__(
        self,
        server='127.0.0.1',
        port='9092',
        topics='',
        group_id=None,
        only_value=True,
        **kwargs
    ):

        super(GetMessagesThread, self).__init__()
        self.daemon = True
        self._is_running = True
        self.only_value = only_value
        self.consumer = KafkaConsumer()
        self.group_id = self.consumer.create_consumer(server=server,
                                                      port=port,
                                                      group_id=group_id,
                                                      **kwargs)

        if not isinstance(topics, list):
            topics = [topics]
        self.consumer.subscribe_topic(self.group_id, topics=topics)
        self.messages = []
        self.messages += self.consumer.poll(group_id=self.group_id, only_value=self.only_value)
        self.start()

    def run(self):
        while self._is_running:
            try:
                self.messages += self.consumer.poll(group_id=self.group_id, only_value=self.only_value)
            except RuntimeError:
                self.consumer.unsubscribe(self.group_id)
                self.consumer.close_consumer(self.group_id)
                self._is_running = False

    def get_messages(self):
        return self.messages[:]

    def clear_messages(self):
        self.messages.clear()


class KafkaConsumer():

    def __init__(self):
        self.consumers = {}

    def create_consumer(
        self,
        group_id=None,
        server="127.0.0.1",
        port="9092",
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
        - ``group_id`` (str or uuid.uuid4() if not set) : name of the consumer group
            to join for dynamic partition assignment (if enabled), and to use for fetching and
            committing offsets. If None, unique string is generated  (via uuid.uuid4())
            and offset commits are disabled. Default: `None`.
        - ``auto_offset_reset`` (str): A policy for resetting offsets on
            OffsetOutOfRange errors: `earliest` will move to the oldest
            available message, `latest` will move to the most recent. Any
            other value will raise the exception. Default: `latest`.
        - ``enable_auto_commit`` (bool): If true the consumer's offset will be
            periodically committed in the background. Default: `True`.
        - ``schema_registry_url`` (str): *required* for Avro Consumer.
            Full URL to avro schema endpoint.

        Note:
        Configuration parameters are described in more detail at
        https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md \n
        """
        if group_id is None:
            group_id = str(uuid.uuid4())

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

        self.consumers[group_id] = consumer
        return group_id

    def create_topic_partition(self, topic_name, partition, offset):
        return TopicPartition(topic_name, partition, offset)

    def get_topic_partitions(self, topic):
        return topic.partitions

    def _is_assigned(self, group_id, topic_partitions):
        for topic_partition in topic_partitions:
            if topic_partition in self.consumers[group_id].assignment():
                return True
        return False

    def assign_to_topic_partition(self, group_id, topic_partitions):
        """Assign a list of TopicPartitions.

        - ``partitions`` (list of `TopicPartition`): Assignment for this instance.
        """
        if isinstance(topic_partitions, TopicPartition):
            topic_partitions = [topic_partitions]
        if not self._is_assigned(group_id, topic_partitions):
            self.consumers[group_id].assign(topic_partitions)

    def subscribe_topic(self, group_id, topics):
        """Subscribe to a list of topics, or a topic regex pattern.

        - ``topics`` (list): List of topics for subscription.
        """

        if not isinstance(topics, list):
            topics = [topics]
        self.consumers[group_id].subscribe(topics)

    def unsubscribe(self, group_id):
        """Unsubscribe of topics.
        """
        self.consumers[group_id].unsubscribe()

    def close_consumer(self, group_id):
        """Close down and terminate the Kafka Consumer.
        """
        self.consumers[group_id].close()

    def seek(self, group_id, topic_partition):
        self.consumers[group_id].seek(topic_partition)

    def poll(
        self,
        group_id,
        timeout=1,
        max_records=1,
        poll_attempts=10,
        only_value=True,
        decode_format=None
    ):
        """Fetch and return messages from assigned topics / partitions as list.
        - ``max_records`` (int): maximum number of messages to get from poll. Default: 1.
        - ``timeout`` (int): Seconds spent waiting in poll if data is not available in the buffer.
        If 0, returns immediately with any records that are available currently in the buffer,
        else returns empty. Must not be negative. Default: `1`
        -  ``poll_attempts`` (int): Attempts to consume messages and endless looping prevention.
        Sometimes the first messages are None or the topic could be empty. Default: `10`.
        """

        messages = []
        while poll_attempts > 0:
            try:
                msg = self.consumers[group_id].poll(timeout=timeout)
            except SerializerError as err:
                print('Message deserialization failed for {}: {}'.format(msg, err))
                break

            if msg is None:
                poll_attempts -= 1
                continue

            if msg.error():
                print(msg.error())
                break

            if only_value:
                messages.append(msg.value())
            else:
                messages.append(msg)

            if len(messages) == max_records:
                break

        if decode_format:
            messages = self._decode_data(data=messages, decode_format=decode_format)

        return messages

    def _decode_data(self, data, decode_format):
        if decode_format:
            return [record.decode(str(decode_format)) for record in data]
        else:
            return data

    # Experimental - getting messages from kafka topic every second
    def start_consumer_threaded(
        self,
        topics,
        group_id=None,
        server='127.0.0.1',
        port='9092',
        only_value=True,
        **kwargs
    ):
        """Run consumer in daemon thread and store data from topics. To read and work with this
           collected data use keyword `Get Messages From Thread`.
           Could be used at the Test setup or in each test.
           This is useful when you are reading always the same topics and you don't want to create
           consumer in each test to poll data. You can create as many consumers in the Test setup
           as you want and then in test just read data with `Get Messages From Thread` keyword.
        - ``topics`` (list): List of topics for subscription.
        - ``group_id`` (str or uuid.uuid4() if not set) : name of the consumer group to join for
            dynamic partition assignment (if enabled), and to use for fetching and
            committing offsets. If None, unique string is generated  (via uuid.uuid4())
            and offset commits are disabled. Default: `None`.
        """
        if group_id is None:
            group_id = str(uuid.uuid4())
        if topics is None:
            raise ValueError("Topics can not be empty!")

        consumer_thread = GetMessagesThread(server, port, topics, group_id=group_id, only_value=only_value, **kwargs)
        return consumer_thread

    def get_messages_from_thread(self, running_thread, decode_format=None):
        """Returns all records gathered from specific thread
        - ``running_thread`` (Thread object) - thread which was executed with
            `Start Consumer Threaded` keyword
        - ``decode_format`` (str) - If you need to decode data to specific format
            (See https://docs.python.org/3/library/codecs.html#standard-encodings). Default: None.
        """
        records = self._decode_data(
            data=running_thread.get_messages(),
            decode_format=decode_format
        )
        return records

    def clear_messages_from_thread(self, running_thread):
        """Remove all records gathered from specific thread
        - ``running_thread`` (Thread object) - thread which was executed with
            `Start Consumer Threaded` keyword
        """
        try:
            running_thread.clear_messages()
        except:
            print('Messages was not removed from thread %s!', running_thread)
