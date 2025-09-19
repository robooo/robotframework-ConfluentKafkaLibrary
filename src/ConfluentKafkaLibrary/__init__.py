import confluent_kafka
from confluent_kafka import ConsumerGroupState

try:
    from confluent_kafka.schema_registry import SchemaRegistryClient
    _SCHEMA_REGISTRY_CLIENT_AVAILABLE = True
    _SCHEMA_REGISTRY_IMPORT_ERROR = None
except ImportError as e:
    _SCHEMA_REGISTRY_IMPORT_ERROR = e
    _SCHEMA_REGISTRY_CLIENT_AVAILABLE = False

from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions, ConfigResource
from robot.libraries.BuiltIn import BuiltIn, RobotNotRunningError
from .consumer import KafkaConsumer
from .producer import KafkaProducer
from .admin_client import KafkaAdminClient
from .version import VERSION

IMPORTS = KafkaConsumer, KafkaProducer, KafkaAdminClient
if _SCHEMA_REGISTRY_CLIENT_AVAILABLE:
    try:
        from .serialization import Serializer, Deserializer
        IMPORTS += Serializer, Deserializer
    except ImportError as e:
        print(e)
        pass

#class ConfluentKafkaLibrary(KafkaConsumer, KafkaProducer, Serializer, Deserializer):
class ConfluentKafkaLibrary(*IMPORTS):
    """ConfluentKafkaLibrary is a Robot Framework library which wraps up
    [https://github.com/confluentinc/confluent-kafka-python | confluent-kafka-python].
    Library supports more functionality like running more clients based on `group_id`
    or running them in threaded mode during the tests, decoding of gathered data etc. (`See` `Examples`).

    This document explains how to use keywords provided by ConfluentKafkaLibrary.
    For information about installation, support, and more, please visit the
    [https://github.com/robooo/robotframework-ConfluentKafkaLibrary | project github page].
    For more information about Robot Framework, see http://robotframework.org.

    == Examples ==
    See [https://github.com/robooo/robotframework-ConfluentKafkaLibrary/tree/master/examples | repo examples].

    *Basic Consumer with predefined group_id*

    | ${group_id}= | `Create Consumer` | group_id=mygroup | # if group_id is not defined uuid4() is generated |
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

    *Handle Byte Data From Topic*

    | ${messages}= | Poll | group_id=${group_id} | max_records=3 | decode_format=utf_8 |
    | ${json} | Convert String to JSON | ${messages}[0] |
    | ${jsonValue} | Get value from JSON | ${json} | $.key |

    """

    ROBOT_LIBRARY_VERSION = VERSION
    ROBOT_LIBRARY_SCOPE = 'GLOBAL'

    def __init__(self):
        KafkaConsumer.__init__(self)
        KafkaProducer.__init__(self)
        KafkaAdminClient.__init__(self)
        self._set_globals_variables_if_robot_running()

    def _set_globals_variables_if_robot_running(self):
        try:
            BuiltIn().set_global_variable('${OFFSET_BEGINNING}', confluent_kafka.OFFSET_BEGINNING)
            BuiltIn().set_global_variable('${OFFSET_END}', confluent_kafka.OFFSET_END)
            BuiltIn().set_global_variable('${OFFSET_STORED}', confluent_kafka.OFFSET_STORED)
            BuiltIn().set_global_variable('${OFFSET_INVALID}', confluent_kafka.OFFSET_INVALID)
            BuiltIn().set_global_variable('${ADMIN_RESOURCE_BROKER}', confluent_kafka.admin.RESOURCE_BROKER)
            BuiltIn().set_global_variable('${ADMIN_RESOURCE_GROUP}', confluent_kafka.admin.RESOURCE_GROUP)
            BuiltIn().set_global_variable('${ADMIN_RESOURCE_TOPIC}', confluent_kafka.admin.RESOURCE_TOPIC)

            BuiltIn().set_global_variable('${CONSUMER_GROUP_STATE_UNKNOWN}', confluent_kafka.ConsumerGroupState.UNKNOWN)
            BuiltIn().set_global_variable('${CONSUMER_GROUP_STATE_PREPARING_REBALANCING}', confluent_kafka.ConsumerGroupState.PREPARING_REBALANCING)
            BuiltIn().set_global_variable('${CONSUMER_GROUP_STATE_COMPLETING_REBALANCING}', confluent_kafka.ConsumerGroupState.COMPLETING_REBALANCING)
            BuiltIn().set_global_variable('${CONSUMER_GROUP_STATE_STABLE}', confluent_kafka.ConsumerGroupState.STABLE)
            BuiltIn().set_global_variable('${CONSUMER_GROUP_STATE_DEAD}', confluent_kafka.ConsumerGroupState.DEAD)
            BuiltIn().set_global_variable('${CONSUMER_GROUP_STATE_EMPTY}', confluent_kafka.ConsumerGroupState.EMPTY)

            BuiltIn().set_global_variable('${CONSUMER_GROUP_TYPE_UNKNOWN}', confluent_kafka.ConsumerGroupType.UNKNOWN)
            BuiltIn().set_global_variable('${CONSUMER_GROUP_TYPE_CONSUMER}', confluent_kafka.ConsumerGroupType.CONSUMER)
            BuiltIn().set_global_variable('${CONSUMER_GROUP_TYPE_CLASSIC}', confluent_kafka.ConsumerGroupType.CLASSIC)
        except RobotNotRunningError as e:
            pass

    def list_topics(self, group_id, topic=None):
        """Request Metadata from cluster. Could be executed with consumer or producer group_id too.
        - ``topic`` (str):  If specified, only request info about this topic, else return for all topics in cluster.
        Default: `None`.
        - ``group_id`` (str): *required* id of the created consumer or producer.
        """
        if group_id is None:
            raise TypeError

        if group_id in self.admin_clients:
            return self.admin_clients[group_id].list_topics().topics
        if group_id in self.consumers:
            return self.consumers[group_id].list_topics(topic).topics
        if group_id in self.producers:
            return self.producers[group_id].list_topics(topic).topics

        raise ValueError('Consumer or producer group_id is wrong or does not exists!')

    def new_topic(self, topic, **kwargs):
        """Instantiate a NewTopic object. Specifies per-topic settings for passing to AdminClient.create_topics().
        - ``topic`` (str): Topic name
        Note: In a multi-cluster production scenario, it is more typical to use a
        replication_factor of 3 for durability.
        """
        return NewTopic(topic=topic, **kwargs)

    def new_partitions(self, topic, **kwargs):
        """Instantiate a NewPartitions object.
        - ``topic`` (str): Topic name
        """
        return NewPartitions(topic=topic, **kwargs)

    def config_resource(self, restype, name, **kwargs):
        """Represents a resource that has configuration, and (optionally) a collection of configuration properties
        for that resource. Used by describe_configs() and alter_configs().
        - ``restype`` (ConfigResource.Type): The resource type.
        -  ``name`` (str): The resource name, which depends on the resource type. For RESOURCE_BROKER,
            the resource name is the broker id.
        """
        return ConfigResource(restype=restype, name=name, **kwargs)

    def get_schema_registry_client(self, conf):
        if not _SCHEMA_REGISTRY_CLIENT_AVAILABLE:
            raise ImportError(
                "SchemaRegistry requires additional dependencies to be installed or one of its transitive dependencies is missing. "
                "Please install with 'pip install robotframework-confluentkafkalibrary[schemaregistry]'.\n"
                "If the error persists, check for missing dependencies in your environment.\n"
                f"ImportError: {_SCHEMA_REGISTRY_IMPORT_ERROR}"
            )
        return SchemaRegistryClient(conf)
