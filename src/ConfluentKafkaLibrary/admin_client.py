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
