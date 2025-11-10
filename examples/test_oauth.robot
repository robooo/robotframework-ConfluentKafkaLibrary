*** Settings ***
Library  ConfluentKafkaLibrary
Library  oauth2_test
Library  Collections
Library  String


*** Variables ***
${SEEN_RF_OAUTH_CB_PRODUCER}  ${False}
${SEEN_RF_OAUTH_CB_CONSUMER}  ${False}
${KAFKA_BOOTSTRAP_SERVERS}    localhost:9092
${TEST_TOPIC}                 oauth2-test-topic


*** Test Cases ***
Test OAuth2 Token Generation
    ${test_token}=  oauth2_test.create_test_token
    Should Not Be Empty  ${test_token}
    ${producer_token_func}=  oauth2_test.get_test_producer_token
    ${consumer_token_func}=  oauth2_test.get_test_consumer_token


Test OAuth2 Library Integration
    ${string_serializer}=  Get String Serializer
    ${oauth_func}=  oauth2_test.get_test_producer_token

    ${status}  ${error}=  Run Keyword And Ignore Error
    ...  Create Producer  localhost:9092
    ...  oauth_cb=${oauth_func}
    ...  security.protocol=sasl_plaintext
    ...  sasl.mechanisms=OAUTHBEARER
    Should Be Equal  ${status}  PASS