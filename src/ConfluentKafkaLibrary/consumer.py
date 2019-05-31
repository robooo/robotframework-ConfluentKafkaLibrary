import sys
import uuid
import copy
import json
import threading
from threading import Thread, Timer
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka import Consumer, KafkaError, TopicPartition


class GetMessagesThread(Thread):

    def __init__(self,
                 server='127.0.0.1',
                 port='9092',
                 topics='',
                 group_id='',
                 func_to_run=None,
                 ):

        super(GetMessagesThread, self).__init__()
        self.daemon = True
        self.consumer = KafkaConsumer()
        self.group_id = getattr(self.consumer, func_to_run)(server=server, port=port, group_id=group_id)
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

    def create_consumer(self,
                        server="127.0.0.1",
                        port="9092",
                        group_id=str(uuid.uuid4()),
                        enable_auto_commit=True,
                        auto_offset_reset="earliest",
                        **kwargs
                        ):

        consumer = Consumer({
            'bootstrap.servers': '{}:{}'.format(server, port),
            'group.id': group_id,
            'enable.auto.commit': enable_auto_commit,
            'default.topic.config': {
                'auto.offset.reset': auto_offset_reset
            },
            **kwargs
        })

        self.__consumers[group_id] = consumer
        return group_id

    def _is_assigned(self, group_id, topic_partition):
        for tp in topic_partition:
            if tp in self.__consumers[group_id].assignment():
                return True
        return False

    def assign_to_topic_partition(self, group_id, topic_partition=None):
        """Assign a list of TopicPartitions.

        - ``partitions`` (list of `TopicPartition`): Assignment for this instance.
        """

        if isinstance(topic_partition, TopicPartition):
            topic_partition = [topic_partition]
        if not self._is_assigned(group_id, topic_partition):
            self.__consumers[group_id].assign(topic_partition)

    def subscribe_topic(self, group_id, topics=[]):
        """Subscribe to a list of topics, or a topic regex pattern.

        - ``topics`` (list): List of topics for subscription.
        """

        if not isinstance(topics, list):
            topics = [topics]
        self.__consumers[group_id].subscribe(topics)

    def unsubscribe_topic(self, group_id):
        """Unsubscribe of topics.
        """
        self.__consumers[group_id].unsubscribe()

    def close_consumer(self, group_id):
        self.__consumers[group_id].close()

    def poll(
        self,
        group_id=None,
        timeout=1,
        max_records=1,
        poll_attempts=10
    ):

        """Fetch messages from assigned topics / partitions.
        - ``max_records`` (int): maximum number of messages to get from poll. Default: 1.
        - ``timeout`` (int): Seconds spent waiting in poll if data is not available in the buffer.
        If 0, returns immediately with any records that are available currently in the buffer, else returns empty.
        Must not be negative. Default: `1`
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
        if decode_format and remove_zero_bytes:
            return [record.decode(str(decode_format)).replace('\x00', '') for record in data]
        elif decode_format and not remove_zero_bytes:
            return [record.decode(str(format)) for record in data]
        elif not decode_format and remove_zero_bytes:
            return [record.replace('\x00', '') for record in data]
        else:
            return data

    # Experimental - getting messages from kafka topic every second
    def start_messages_threaded(self, server='127.0.0.1', port='9092', topics='', group_id=None, func_to_run='connect_consumer'):
        if group_id is None:
            group_id = str(uuid.uuid4())

        t = GetMessagesThread(server, port, topics, group_id=group_id, func_to_run=func_to_run)
        t.start()
        t.join()
        return t

    def get_messages_from_thread(self, running_thread, decode_format=None, remove_zero_bytes=False):
        records = self.decode_data(data=running_thread.messages, decode_format=decode_format, remove_zero_bytes=remove_zero_bytes)
        return records

    def stop_thread(self, running_thread):
        running_thread.stop()
