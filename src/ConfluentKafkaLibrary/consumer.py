import uuid
from threading import Thread
from confluent_kafka import Consumer, KafkaException, KafkaError, TopicPartition
from confluent_kafka import DeserializingConsumer
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.admin import AdminClient


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

        super().__init__()
        self.daemon = True
        self.server = server
        self.port = port
        self._is_running = True
        self.only_value = only_value
        self.consumer = KafkaConsumer()
        self.group_id = self.consumer.create_consumer(group_id=group_id,
                                                      server=server,
                                                      port=port,
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

    def get_group_id(self):
        return self.group_id

    def get_messages(self):
        return self.messages[:]

    def clear_messages(self):
        self.messages.clear()

    def stop_consumer(self):
        self._is_running = False
        self.join()
        self.consumer.unsubscribe(self.group_id)
        self.consumer.close_consumer(self.group_id)
        admin_client = AdminClient({'bootstrap.servers': f'{self.server}:{self.port}'})
        response = admin_client.delete_consumer_groups([self.group_id], request_timeout=10)
        try:
            response[self.group_id].result()
        except Exception as e:
            return e
        return response[self.group_id].exception()


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
        auto_create_topics=True,
        key_deserializer=None,
        value_deserializer=None,
        legacy=True,
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
        - ``auto_create_topics`` (bool): Consumers no longer trigger auto creation of topics,
            will be removed in future release. Default: `True`.
        - ``legacy`` (bool): Activate SerializingConsumer if 'False' else
            AvroConsumer (legacy) is used. Will be removed when confluent-kafka will deprecate this.
            Default: `True`.

        Note:
        Configuration parameters are described in more detail at
        https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md \n
        """
        if group_id is None:
            group_id = str(uuid.uuid4())

        if schema_registry_url and legacy:
            consumer = AvroConsumer({
                'bootstrap.servers': '{}:{}'.format(server, port),
                'group.id': group_id,
                'enable.auto.commit': enable_auto_commit,
                'allow.auto.create.topics': auto_create_topics,
                'auto.offset.reset': auto_offset_reset,
                'schema.registry.url': schema_registry_url,
                **kwargs})
        elif not legacy:
            consumer = DeserializingConsumer({
                'bootstrap.servers': '{}:{}'.format(server, port),
                'group.id': group_id,
                'enable.auto.commit': enable_auto_commit,
                'auto.offset.reset': auto_offset_reset,
                'key.deserializer': key_deserializer,
                'value.deserializer': value_deserializer,
                **kwargs})
        else:
            consumer = Consumer({
                'bootstrap.servers': '{}:{}'.format(server, port),
                'group.id': group_id,
                'enable.auto.commit': enable_auto_commit,
                'allow.auto.create.topics': auto_create_topics,
                'auto.offset.reset': auto_offset_reset,
                **kwargs})

        self.consumers[group_id] = consumer
        return group_id

    def get_all_consumers(self):
        """Returns all non-threaded consumers
        """
        return self.consumers

    def create_topic_partition(self, topic_name, partition=None, offset=None):
        """Returns TopicPartiton object based on
           https://docs.confluent.io/current/clients/confluent-kafka-python/#topicpartition

            - ``topic_name`` (str): Topic name.
            - ``partition`` (int): Partition id.
            - ``offset`` (int): Initial partition offset.
        """
        if partition is not None and offset is not None:
            return TopicPartition(topic_name, partition, offset)
        elif partition is None:
            return TopicPartition(topic_name, offset)
        elif offset is None:
            return TopicPartition(topic_name, partition)
        return TopicPartition(topic_name)

    def get_topic_partitions(self, topic):
        """Returns dictionary of all TopicPartitons in topic (topic.partitions).
        """
        return topic.partitions

    def subscribe_topic(self, group_id, topics, **kwargs):
        """Subscribe to a list of topics, or a topic regex pattern.
           https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Consumer.subscribe

        - ``topics`` (list): List of topics for subscription.
        """
        if not isinstance(topics, list):
            topics = [topics]
        self.consumers[group_id].subscribe(topics, **kwargs)

    def get_watermark_offsets(self, group_id, topic_partition, **kwargs):
        """Retrieve low and high offsets for partition.
        """
        if not isinstance(topic_partition, TopicPartition):
            raise TypeError('topic_partition needs to be TopicPartition() type!')
        return self.consumers[group_id].get_watermark_offsets(topic_partition, **kwargs)

    def get_assignment(self, group_id):
        return self.consumers[group_id].assignment()

    def assign_to_topic_partition(self, group_id, topic_partitions):
        """Assign a list of TopicPartitions.

        - ``topic_partitions`` (`TopicPartition` or list of `TopicPartition`): Assignment for this instance.
        """
        if isinstance(topic_partitions, TopicPartition):
            topic_partitions = [topic_partitions]
        for topic_partition in topic_partitions:
            if topic_partition not in self.consumers[group_id].assignment():
                self.consumers[group_id].assign(topic_partitions)

    def unassign(self, group_id):
        self.consumers[group_id].unassign()

    def unsubscribe(self, group_id):
        """Unsubscribe of topics.
        """
        self.consumers[group_id].unsubscribe()

    def close_consumer(self, group_id):
        """Close down and terminate the Kafka Consumer.
        """
        self.consumers[group_id].close()
        del self.consumers[group_id]

    def seek(self, group_id, topic_partition):
        """https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Consumer.seek
        """
        return self.consumers[group_id].seek(topic_partition)

    def get_position(self, group_id, topic_partitions):
        """Retrieve current positions (offsets) for the list of partitions.

            - ``topic_partitions`` (`TopicPartition` or list of `TopicPartition`): Assignment for this instance.
        """
        if isinstance(topic_partitions, TopicPartition):
            topic_partitions = [topic_partitions]
        return self.consumers[group_id].position(topic_partitions)

    def pause(self, group_id, topic_partitions):
        """Pause consumption for the provided list of partitions.
        """
        if isinstance(topic_partitions, TopicPartition):
            topic_partitions = [topic_partitions]
        self.consumers[group_id].pause(topic_partitions)

    def resume(self, group_id, topic_partitions):
        """Resume consumption for the provided list of partitions.
        """
        if isinstance(topic_partitions, TopicPartition):
            topic_partitions = [topic_partitions]
        self.consumers[group_id].resume(topic_partitions)

    def store_offsets(self, group_id, **kwargs):
        """Store offsets for a message or a list of offsets.
        """
        self.consumers[group_id].store_offsets(**kwargs)

    def poll(
        self,
        group_id,
        timeout=1,
        max_records=1,
        poll_attempts=10,
        only_value=True,
        auto_create_topics=True,
        decode_format=None,
        fail_on_deserialization=False
    ):
        """Fetch and return messages from assigned topics / partitions as list.
        - ``timeout`` (int): Seconds spent waiting in poll if data is not available in the buffer.\n
        - ``max_records`` (int): maximum number of messages to get from poll. Default: 1.
        If 0, returns immediately with any records that are available currently in the buffer,
        else returns empty. Must not be negative. Default: `1`
        - ``poll_attempts`` (int): Attempts to consume messages and endless looping prevention.
        Sometimes the first messages are None or the topic could be empty. Default: `10`.
        - ``only_value`` (bool): Return only message.value(). Default: `True`.
        - ``decode_format`` (str) - If you need to decode data to specific format
            (See https://docs.python.org/3/library/codecs.html#standard-encodings). Default: None.
        - ``auto_create_topics`` (bool): Consumers no longer trigger auto creation of topics,
            will be removed in future release. If True then the error message UNKNOWN_TOPIC_OR_PART is ignored.
            Default: `True`.
        - ``fail_on_deserialization`` (bool): If True and message deserialization fails, will raise a SerializerError
            exception; on False will just stop the current poll and return the message so far. Default: `False`.
        """

        messages = []
        while poll_attempts > 0:
            msg = None
            try:
                msg = self.consumers[group_id].poll(timeout=timeout)
            except SerializerError as err:
                error = 'Message deserialization failed for {}: {}'.format(msg, err)
                if fail_on_deserialization:
                    raise SerializerError(error)

                print(error)
                break

            if msg is None:
                poll_attempts -= 1
                continue

            if msg.error():
                # Workaround due to new message return + deprecation of the "Consumers no longer trigger auto creation of topics"
                if int(msg.error().code()) == KafkaError.UNKNOWN_TOPIC_OR_PART and auto_create_topics:
                    continue
                raise KafkaException(msg.error())

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

    def get_thread_group_id(self, running_thread):
        return running_thread.get_group_id()

    def clear_messages_from_thread(self, running_thread):
        """Remove all records gathered from specific thread
        - ``running_thread`` (Thread object) - thread which was executed with
            `Start Consumer Threaded` keyword
        """
        try:
            running_thread.clear_messages()
        except Exception as e:
            return f"Messages were not removed from thread {running_thread}!\n{e}"

    def stop_consumer_threaded(self, running_thread):
        resp = running_thread.stop_consumer()
        return resp