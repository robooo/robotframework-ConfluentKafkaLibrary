from .consumer import KafkaConsumer
from .producer import KafkaProducer
from .version import VERSION


class ConfluentKafkaLibrary(KafkaConsumer, KafkaProducer):
    """ConfluentKafkaLibrary is a Robot Framework library which wraps up
    [https://github.com/confluentinc/confluent-kafka-python | confluent-kafka-python].
    Keywords are inspired by [https://github.com/s4int/robotframework-KafkaLibrary | robotframework-KafkaLibrary]
    which is based on [https://github.com/Parsely/pykafka | pykafka project]
    and support more functionality like running more clients based on `group_id` or running them in threaded mode
    during the tests, decoding of gathered data etc. (`See` `Examples`).

    This document explains how to use keywords provided by ConfluentKafkaLibrary.
    For information about installation, support, and more, please visit the
    [https://github.com/robooo/robotframework-ConfluentKafkaLibrary | project github page].
    For more information about Robot Framework, see http://robotframework.org.

    == Examples ==
    
    *Basic Consumer with predefined group_id*

    | ${group_id}= | `Create Consumer` | group_id=mygroup | # if group_id is not defined uuid4() is gemerated |
    | `Subscribe Topic` | group_id=${group_id} | topics=test_topic |
    | ${result}= | `Poll` | group_id=${group_id} | max_records=5 |
    | `Log` | ${result} |
    | `Unsubscribe` | ${group_id} |
    | `Close Consumer` | ${group_id} |

    *More Consumers*

    | ${group_id_1}= | `Create Consumer` |
    | `Subscribe Topic` | group_id=${group_id_1} | topics=topic1 |
    | ${group_id_2}= | `Create Consumer` |
    | `Subscribe Topic` | group_id=${group_id_2} | topics=topic2 |
    | ${result_1}= | `Poll` | group_id=${group_id_1} | max_records=5 |
    | ${result_2}= | `Poll` | group_id=${group_id_2} | max_records=2 |
    | `Unsubscribe` | ${group_id_1} |
    | `Unsubscribe` | ${group_id_2} |
    | `Close Consumer` | ${group_id_1} |
    | `Close Consumer` | ${group_id_2} |

    *Run Avro Consumer over HTTPS and threaded:*

    | ${thread}= | `Start Consumer Threaded` |
    | | ...  topics=test_topic |
    | | ...  schema_registry_url=https://localhost:8081 |
    | | ...  auto_offset_reset=earliest | # We want to get all messages |
    | | ...  security.protocol=ssl |
    | | ...  schema.registry.ssl.ca.location=/home/user/cert/caproxy.pem |
    | | ...  ssl.ca.location=/home/user/cert/caproxy.pem |
    | | ...  ssl.certificate.location=/home/user/cert/kafka-client-cert.pem |
    | | ...  ssl.key.location=/home/user/cert/kafka-client-key.pem |
    | `Log` | `Execute commands which should push some data to topic` |
    | ${messages}= | `Get Messages From Thread` | ${thread} |
    | Stop Thread | ${thread} |

    """

    ROBOT_LIBRARY_VERSION = VERSION
    ROBOT_LIBRARY_SCOPE = 'GLOBAL'

    def __init__(self):
        KafkaConsumer.__init__(self)
        KafkaProducer.__init__(self)

    def list_topics(self, group_id, topic=None):
        """Request Metadata from cluster. Could be executed with consumer or producer group_id too.
        - ``topic`` (str):  If specified, only request info about this topic, else return for all topics in cluster. Default: `None`.
        - ``group_id`` (str): *required* id of the created consumer or producer.
        """
        if group_id is None:
            raise TypeError

        if group_id in self.consumers:
            return self.consumers[group_id].list_topics(topic).topics
        elif group_id in self.producers:
            return self.producers[group_id].list_topics(topic).topics
        else:
            raise ValueError('Consumer or producer group_id is wrong or does not exists!')
