import uuid
from confluent_kafka.admin import AdminClient
from confluent_kafka import KafkaException


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

    def list_groups(self, group_id, states=None, request_timeout=10):
        """List consumer groups.
        - ``states`` (list(ConsumerGroupState)): filter consumer groups which are currently in these states.
            For example usage see 'AdminClient List Consumer Groups' at
            examples/test_adminclient.py
            Default: `None`.
        - ``request_timeout`` (int): Maximum response time before timing out.
            Default: `10`.
        """
        if states is None:
            states = []
        future = self.admin_clients[group_id].list_consumer_groups(request_timeout=request_timeout, states=set(states))
        return future.result()

    def describe_groups(self, group_id, group_ids, request_timeout=10):
        """Describe consumer groups.
        - ``group_ids`` (list(str)): List of group_ids which need to be described.
        - ``request_timeout`` (int): Maximum response time before timing out.
            Default: `10`.
        """
        response = self.admin_clients[group_id].describe_consumer_groups(group_ids, request_timeout=request_timeout)

        groups_results={}
        for con_id in group_ids:
            try:
                if response[con_id].exception() is None:
                    groups_results[con_id] = response[con_id].result()
                else:
                    groups_results[con_id] = response[con_id].exception()
            except KafkaException as e:
                return f"Failed to describe group {con_id}: {e}"
            except (TypeError, ValueError ) as e:
                return f"Invalid input: {e}"
        return groups_results

    def delete_groups(self, group_id, group_ids, request_timeout=10):
        """Delete the given consumer groups.
        - ``group_ids`` (list(str)): List of group_ids which need to be deleted.
        - ``request_timeout`` (int): Maximum response time before timing out.
            Default: `10`.
        """
        response = self.admin_clients[group_id].delete_consumer_groups(group_ids, request_timeout=request_timeout)

        groups_results={}
        for con_id in group_ids:
            try:
                if response[con_id].exception() is None:
                    groups_results[con_id] = response[con_id].result()
                else:
                    groups_results[con_id] = response[con_id].exception()
            except KafkaException as e:
                return f"Failed to delete group {con_id}: {e}"
            except (TypeError, ValueError ) as e:
                return f"Invalid input: {e}"
        return groups_results

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

        topics_results={}
        for topic, f in fs.items():
            try:
                if f.exception() is None:
                    topics_results[topic] = f.result()
                else:
                    topics_results[topic] = f.exception()
            except KafkaException as e:
                return f"Failed to create topic {topic}: {e}"
            except (TypeError, ValueError ) as e:
                return f"Invalid input: {e}"
        return topics_results

    def delete_topics(self, group_id, topics, **kwargs):
        if isinstance(topics, str):
            topics = [topics]

        fs = self.admin_clients[group_id].delete_topics(topics, **kwargs)

        topics_results={}
        for topic, f in fs.items():
            try:
                if f.exception() is None:
                    topics_results[topic] = f.result()
                else:
                    topics_results[topic] = f.exception()
            except KafkaException as e:
                return f"Failed to delete topic {topic}: {e}"
            except (TypeError, ValueError ) as e:
                return f"Invalid input: {e}"
        return topics_results

    def create_partitions(self, group_id, new_partitions, **kwargs):
        """Create additional partitions for the given topics.
        - ``new_partitions``  (list(NewPartitions) or NewPartitions): New partitions to be created.
        """
        fs = None
        if isinstance(new_partitions, list):
            fs = self.admin_clients[group_id].create_partitions(new_partitions, **kwargs)
        else:
            fs = self.admin_clients[group_id].create_partitions([new_partitions], **kwargs)

        partitions_results={}
        for partition, f in fs.items():
            try:
                if f.exception() is None:
                    partitions_results[partition] = f.result()
                else:
                    partitions_results[partition] = f.exception()
            except KafkaException as e:
                return f"Failed to add partitions to topic {partition}: {e}"
            except (TypeError, ValueError ) as e:
                return f"Invalid input: {e}"
        return partitions_results

    def describe_configs(self, group_id, resources, **kwargs):
        """Get the configuration of the specified resources.
        - ``resources``  (list(ConfigResource) or ConfigResource): Resources to get the configuration for.
        """
        fs = None
        if isinstance(resources, list):
            fs = self.admin_clients[group_id].describe_configs(resources, **kwargs)
        else:
            fs = self.admin_clients[group_id].describe_configs([resources], **kwargs)

        config_results={}
        for config, f in fs.items():
            try:
                if f.exception() is None:
                    config_results[config.name] = f.result()
                else:
                    config_results[config.name] = f.exception()
            except KafkaException as e:
                return f"Failed to describe config {config.name}: {e}"
            except (TypeError, ValueError ) as e:
                return f"Invalid input: {e}"
        return config_results

    def alter_configs(self, group_id, resources, **kwargs):
        """Update configuration properties for the specified resources.
        - ``resources``  (list(ConfigResource) or ConfigResource): Resources to update configuration of.
        """
        fs = None
        if isinstance(resources, list):
            fs = self.admin_clients[group_id].alter_configs(resources, **kwargs)
        else:
            fs = self.admin_clients[group_id].alter_configs([resources], **kwargs)

        config_results={}
        for config, f in fs.items():
            try:
                if f.exception() is None:
                    config_results[config.name] = f.result()
                else:
                    config_results[config.name] = f.exception()
            except KafkaException as e:
                return f"Failed to alter config {config.name}: {e}"
            except (TypeError, ValueError ) as e:
                return f"Invalid input: {e}"
        return config_results