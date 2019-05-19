from .consumer import KafkaConsumer
from .version import VERSION


class ConfluentKafkaLibrary(KafkaConsumer):
    ROBOT_LIBRARY_VERSION = VERSION
    ROBOT_LIBRARY_SCOPE = 'GLOBAL'

