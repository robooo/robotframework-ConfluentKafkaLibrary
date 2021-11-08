import uuid
from confluent_kafka.admin import AdminClient


class KafkaAdminClient():

    def __init__(self):
        self.admin_clients = {}

    def create_admin_client(
        self,
        group_id=None,
        server="127.0.0.1",
        port="9092",
        **kwargs
    ):
        if group_id is None:
            group_id = str(uuid.uuid4())

        admin_client = AdminClient({
            'bootstrap.servers': '{}:{}'.format(server, port),
            **kwargs})

        self.admin_clients[group_id] = admin_client
        return group_id

    def create_topics(self, group_id, new_topics, **kwargs):
        """Create one or more new topics and wait for each one to finish.
        - ``new_topics`` (list(NewTopic) or NewTopic): A list of specifications (NewTopic)
            or a single instance for the topics that should be created.
        """
        fs = None
        if isinstance(new_topics, list):
            fs = self.admin_clients[group_id].create_topics(new_topics, **kwargs)
        else:
            fs = self.admin_clients[group_id].create_topics([new_topics], **kwargs)

        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                print("Topic {} created".format(topic))
            except Exception as e:
                raise Exception("Failed to create topic {}: {}".format(topic, e))

    def delete_topics(self, group_id, topics, **kwargs):
        if isinstance(topics, str):
            topics = [topics]

        fs = self.admin_clients[group_id].delete_topics(topics, **kwargs)

        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                print("Topic {} deleted".format(topic))
            except Exception as e:
                raise Exception("Failed to delete topic {}: {}".format(topic, e))

    def create_partitions(self, group_id, new_partitions, **kwargs):
        """Create additional partitions for the given topics.
        - ``new_partitions``  (list(NewPartitions) or NewPartitions): New partitions to be created.
        """
        fs = None
        if isinstance(new_partitions, list):
            fs = self.admin_clients[group_id].create_partitions(new_partitions, **kwargs)
        else:
            fs = self.admin_clients[group_id].create_partitions([new_partitions], **kwargs)

        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                print("Additional partitions created for topic {}".format(topic))
            except Exception as e:
                raise Exception("Failed to add partitions to topic {}: {}".format(topic, e))

    def describe_configs(self, group_id, resources, **kwargs):
        """Get the configuration of the specified resources.
        - ``resources``  (list(ConfigResource) or ConfigResource): Resources to get the configuration for.
        """
        fs = None
        if isinstance(resources, list):
            fs = self.admin_clients[group_id].describe_configs(resources, **kwargs)
        else:
            fs = self.admin_clients[group_id].describe_configs([resources], **kwargs)

        for res, f in fs.items():
            try:
                configs = f.result()
                return configs

            except KafkaException as e:
                raise KafkaException("Failed to describe {}: {}".format(res, e))
            except Exception:
                raise

    def alter_configs(self, group_id, resources, **kwargs):
        """Update configuration properties for the specified resources.
        - ``resources``  (list(ConfigResource) or ConfigResource): Resources to update configuration of.
        """
        fs = None
        if isinstance(resources, list):
            fs = self.admin_clients[group_id].alter_configs(resources, **kwargs)
        else:
            fs = self.admin_clients[group_id].alter_configs([resources], **kwargs)

        for res, f in fs.items():
            try:
                f.result()  # The result itself is None
                print("{} configuration successfully altered".format(res))
            except Exception:
                raise
