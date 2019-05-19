import sys
import uuid
import logging
#from robot.api.deco import keyword
from confluent_kafka import Consumer, KafkaError, TopicPartition


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

    def print_hello(self):
        print('aa')
