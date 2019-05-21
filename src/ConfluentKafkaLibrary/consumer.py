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
                 ):

        super(GetMessagesThread, self).__init__()
        self.daemon = True
        self.consumer = KafkaConsumer()
        self.group_id = self.consumer.create_consumer(server=server, port=port)
        if not isinstance(topics, list):
            topics = [topics]
        self.consumer.subscribe_topic(self.group_id, topics=topics)
        self.messages = self.consumer.get_messages(group_id=self.group_id)

    def run(self):
        threading.Timer(1, self.run).start()
        self.messages += self.consumer.get_messages(group_id=self.group_id)

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
            }
        })

        self.__consumers[group_id] = consumer
        return group_id

    def assign_to_topic_partition(self, group_id, topic_partition=None):
        """Assign a list of TopicPartitions.

        - ``partitions`` (list of `TopicPartition`): Assignment for this instance.
        """

        if isinstance(topic_partition, TopicPartition):
            topic_partition = [topic_partition]
        if not self._is_assigned(topic_partition):
            self.__consumers[group_id].assign(topic_partition)

    def subscribe_topic(self, group_id, topics=[]):
        """Subscribe to a list of topics, or a topic regex pattern.

        - ``topics`` (list): List of topics for subscription.
        """

        if not isinstance(topics, list):
            topics = [topics]
        self.__consumers[group_id].subscribe(topics)

    def unsubscribe_topic(self, group_id, topics=[]):
        """Unsubscribe to a list of topics, or a topic regex pattern.

        - ``topics`` (list): List of topics for subscription.
        """

        if not isinstance(topics, list):
            topics = [topics]
        self.__consumers[group_id].unsubscribe(topics)

    def close_consumer(self, group_id):
        self.__consumers[group_id].close()

    def get_messages(self, max_records=499, timeout=1, data_type=None, empty_stabilization_polls=5, group_id=None):
        messages = []

        while empty_stabilization_polls > 0:
            try:
                msg = self.__consumers[group_id].poll(timeout=timeout)
            except SerializerError as e:
                print('Message deserialization failed for {}: {}'.format(msg, e))
                break

            if msg is None:
                empty_stabilization_polls -= 1
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    empty_stabilization_polls = 0
                    continue
                else:
                    print(msg.error())
                    break

            if len(messages) == max_records:
                return list(messages)

            messages.append(msg.value())

        return list(messages)

    # Experimental - getting messages from kafka topic every second
    def start_messages_threaded(self, server='127.0.0.1', port='9092', topics=''):
        t = GetMessagesThread(server, port, topics)
        t.start()
        t.join()
        return t

    def get_messages_threaded(self, running_thread):
        return running_thread.messages

    def stop_thread(self, running_thread):
        return running_thread.stop()
