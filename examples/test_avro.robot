*** Settings ***
Library  ConfluentKafkaLibrary
Library  Collections
Library  String

Suite Setup  Starting Test
Suite Teardown  Stop Thread


*** Test Cases ***
Avro Producer With Schemas As String Argument
    [Setup]  Clear Messages From Thread  ${MAIN_THREAD}
    ${value_schema}=  Set Variable  {"namespace": "example.avro","type": "record","name": "User","fields": [{"name": "name","type": "string"},{"name": "number","type": ["int","null"]}]}
    ${key_schema}=  Set Variable  {"namespace": "example.avro","type": "record","name": "User","fields": [{"name": "name","type": "string"}]}
    ${producer_id}=  Create Producer  schema_registry_url=http://127.0.0.1:8081
    ...  value_schema=${value_schema}  key_schema=${key_schema}
    ${value}=  Create Dictionary  name=Robot  number=${10}
    Produce  group_id=${producer_id}  topic=avro_testing1  partition=${0}  value=${value}  key=${KEY_FOR_SCHEMA}
    Wait Until Keyword Succeeds  10x  0.5s  All Messages Are Delivered  ${producer_id}
    Sleep  1s

    ${consumer_group_id}=  Create Consumer  auto_offset_reset=earliest  schema_registry_url=http://127.0.0.1:8081
    Subscribe Topic  group_id=${consumer_group_id}  topics=avro_testing1
    ${messages}=  Poll  group_id=${consumer_group_id}
    Should Be Equal  ${TEST_DATA}  ${messages}
    ${thread_messages}=  Get Messages From Thread  ${MAIN_THREAD}
    Should Be Equal  ${TEST_DATA}  ${thread_messages}
    [Teardown]  Basic Teardown  ${consumer_group_id}

Avro Producer With Path To Schemas
    [Setup]  Clear Messages From Thread  ${MAIN_THREAD}
    ${value_schema_file_path}=  Set Variable  examples/schema/avro/ValueSchema.avsc
    ${key_schema_file_path}=  Set Variable  examples/schema/avro/KeySchema.avsc
    ${producer_id}=  Create Producer  schema_registry_url=http://127.0.0.1:8081
    ...  value_schema=${value_schema_file_path}  key_schema=${key_schema_file_path}
    ${value}=  Create Dictionary  name=Robot  number=${10}
    Produce  group_id=${producer_id}  topic=avro_testing2  partition=${0}  value=${value}  key=${KEY_FOR_SCHEMA}
    Wait Until Keyword Succeeds  10x  0.5s  All Messages Are Delivered  ${producer_id}
    Sleep  1s

    ${consumer_group_id}=  Create Consumer  auto_offset_reset=earliest  schema_registry_url=http://127.0.0.1:8081
    Subscribe Topic  group_id=${consumer_group_id}  topics=avro_testing2
    ${messages}=  Poll  group_id=${consumer_group_id}
    Should Be Equal  ${TEST_DATA}  ${messages}
    ${thread_messages}=  Get Messages From Thread  ${MAIN_THREAD}
    Should Be Equal  ${TEST_DATA}  ${thread_messages}
    [Teardown]  Basic Teardown  ${consumer_group_id}

Avro Producer Consumer With Serializers
    [Setup]  Clear Messages From Thread  ${MAIN_THREAD}
    ${schema_registry_conf}=  Create Dictionary  url=http://127.0.0.1:8081
    ${schema_registry_client}=  Get Schema Registry Client  ${schema_registry_conf}
    ${schema_str}=  Set Variable  {"namespace": "example.avro","type": "record","name": "User","fields": [{"name": "name","type": "string"},{"name": "number","type": ["int","null"]}]}
    ${avro_serializer}=  Get Avro Serializer  ${schema_str}  ${schema_registry_client}
    ${avro_deserializer}=  Get Avro Deserializer  ${schema_str}  ${schema_registry_client}
    ${string_serializer}=  Get String Serializer
    ${string_deserializer}=  Get String Deserializer

    ${producer_id}=  Create Producer  key_serializer=${string_serializer}  value_serializer=${avro_serializer}  legacy=${False}
    ${value}=  Create Dictionary  name=Robot  number=${10}
    Produce  group_id=${producer_id}  topic=avro_testing3  partition=${0}  value=${value}  key=568a68fd-2785-44cc-8997-1295c3755d28
    Wait Until Keyword Succeeds  10x  0.5s  All Messages Are Delivered  ${producer_id}

    ${consumer_group_id}=  Create Consumer  auto_offset_reset=latest  key_deserializer=${string_deserializer}  value_deserializer=${avro_deserializer}  legacy=${False}
    Subscribe Topic  group_id=${consumer_group_id}  topics=avro_testing3
    Poll  group_id=${consumer_group_id}  # Dummy poll when using offset reset 'latest'

    Produce  group_id=${producer_id}  topic=avro_testing3  value=${value}  partition=${0}  key=568a68fd-2785-44cc-8997-1295c3755d28
    Wait Until Keyword Succeeds  10x  0.5s  All Messages Are Delivered  ${producer_id}
    ${messages}=  Poll  group_id=${consumer_group_id}
    Should Be Equal  ${messages}  ${TEST_DATA}
    [Teardown]  Basic Teardown  ${consumer_group_id}


*** Keywords ***
Starting Test
    Set Suite Variable  @{TEST_TOPIC}  avro_testing1  avro_testing2  avro_testing3
    Set Suite Variable  ${KEY}  568a68fd-2785-44cc-8997-1295c3755d28
    Set Suite Variable  &{KEY_FOR_SCHEMA}  name=testkey

    ${value}=  Create Dictionary  name=Robot  number=${10}
    ${data}=  Create List  ${value}
    Set Suite Variable  ${TEST_DATA}  ${data}

    # Rewrite to support topic creation via AdminClient
    ${value_schema}=  Set Variable  {"namespace": "example.avro","type": "record","name": "User","fields": [{"name": "name","type": "string"},{"name": "number","type": ["int","null"]}]}
    ${key_schema}=  Set Variable  {"namespace": "example.avro","type": "record","name": "User","fields": [{"name": "name","type": "string"}]}
    ${producer_id}=  Create Producer  schema_registry_url=http://127.0.0.1:8081
    ...  value_schema=${value_schema}  key_schema=${key_schema}

    Produce  group_id=${producer_id}  topic=avro_testing1  partition=${0}  value=${value}  key=${KEY_FOR_SCHEMA}
    Wait Until Keyword Succeeds  10x  0.5s  All Messages Are Delivered  ${producer_id}
    Produce  group_id=${producer_id}  topic=avro_testing2  partition=${0}  value=${value}  key=${KEY_FOR_SCHEMA}
    Wait Until Keyword Succeeds  10x  0.5s  All Messages Are Delivered  ${producer_id}

    ${thread}=  Start Consumer Threaded  topics=${TEST_TOPIC}   schema_registry_url=http://127.0.0.1:8081  auto_offset_reset=latest
    Set Suite Variable  ${MAIN_THREAD}  ${thread}

All Messages Are Delivered
    [Arguments]  ${producer_id}
    ${count}=  Flush  ${producer_id}
    Log  Reaming messages to be delivered: ${count}
    Should Be Equal As Integers  ${count}  0

Basic Teardown
    [Arguments]  ${group_id}
    Unsubscribe  ${group_id}
    Close Consumer  ${group_id}
    ${groups}=  Create List  ${group_id}
    ${admin_client_id}=  Create Admin Client
    ${resp}=  Delete Groups  ${admin_client_id}  group_ids=${groups}
    Log  ${resp}

Stop Thread
    ${resp}=  Stop Consumer Threaded  ${MAIN_THREAD}
    Log  ${resp}