from confluent_kafka.schema_registry.avro import *
from confluent_kafka.schema_registry.protobuf import (ProtobufSerializer, ProtobufDeserializer)
from confluent_kafka.schema_registry.json_schema import JSONSerializer, JSONDeserializer
from confluent_kafka.serialization import *


class Serializer():

    def get_avro_serializer(self, schema_str, schema_registry_client, to_dict=None, conf=None):
        return AvroSerializer(schema_registry_client, schema_str, to_dict, conf)

    def get_double_serializer(self):
        return DoubleSerializer()

    def get_integer_serializer(self):
        return IntegerSerializer()

    def get_json_serializer(self, schema_str, schema_registry_client, to_dict=None, conf=None):
        return JSONSerializer(schema_str, schema_registry_client, to_dict, conf)

    def get_protobuf_serializer(self, msg_type, schema_registry_client, conf=None):
        base_conf = {'use.deprecated.format': False}
        if conf is None:
            conf = base_conf.copy()
        else:
            conf.update(base_conf)

        return ProtobufSerializer(msg_type, schema_registry_client, conf)

    def get_string_serializer(self, codec='utf_8'):
        return StringSerializer(codec)


class Deserializer():

    def get_avro_deserializer(self, schema_str, schema_registry_client, from_dict=None):
        return AvroDeserializer(schema_registry_client, schema_str, from_dict)

    def get_double_deserializer(self):
        return DoubleDeserializer()

    def get_integer_deserializer(self):
        return IntegerDeserializer()

    def get_json_deserializer(self, schema_str, from_dict=None):
        return JSONDeserializer(schema_str, from_dict)

    def get_protobuf_deserializer(self, message_type):
        return ProtobufDeserializer(message_type, {'use.deprecated.format': False})

    def get_string_deserializer(self, codec='utf_8'):
        return StringDeserializer(codec)
