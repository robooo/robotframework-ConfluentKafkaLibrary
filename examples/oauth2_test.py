import time
import functools
import json
from robot.libraries.BuiltIn import BuiltIn


def create_test_token():
    """Create a test OAuth2 token for unsecured testing"""
    token_payload = {
        "sub": "test-user",
        "iss": "test-issuer",
        "aud": "kafka",
        "exp": int(time.time()) + 3600,
        "iat": int(time.time()),
        "scope": "kafka-producer kafka-consumer"
    }

    import base64
    token_json = json.dumps(token_payload)
    test_token = base64.b64encode(token_json.encode()).decode()

    return test_token


def oauth_cb_test_producer(oauth_config):
    BuiltIn().set_global_variable("${SEEN_RF_OAUTH_CB_PRODUCER}", True)
    test_token = create_test_token()
    expiry_time = time.time() + 3600
    BuiltIn().log(f"Generated test token: {test_token[:50]}...")
    return test_token, expiry_time


def oauth_cb_test_consumer(oauth_config):
    BuiltIn().set_global_variable("${SEEN_RF_OAUTH_CB_CONSUMER}", True)
    test_token = create_test_token()
    expiry_time = time.time() + 3600

    return test_token, expiry_time


def get_test_producer_token(oauth_config=None):
    return functools.partial(oauth_cb_test_producer, oauth_config or {})


def get_test_consumer_token(oauth_config=None):
    return functools.partial(oauth_cb_test_consumer, oauth_config or {})