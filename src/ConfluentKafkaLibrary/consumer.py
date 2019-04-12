import sys
import uuid
import logging
from robot.api.deco import keyword
from confluent_kafka import Consumer, KafkaError

class Consumer(object):
    ROBOT_LIBRARY_SCOPE = 'TEST_SUITE'

    def __init__(self):
        self.__consumers = {}
    
    def print_hello(self):
        print('aa')

