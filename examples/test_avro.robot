*** Settings ***
Library  ConfluentKafkaLibrary
Library  Collections
Library  String

Suite Setup  Starting Test


*** Test Cases ***
Avro Producer Consumer With Serializers
    ${schema_registry_conf}=  Create Dictionary  url=http://127.0.0.1:8081
    ${schema_registry_client}=  Get Schema Registry Client  ${schema_registry_conf}
    ${schema_str}=  Set Variable  {"namespace": "example.avro","type": "record","name": "User","fields": [{"name": "name","type": "string"},{"name": "number","type": ["int","null"]}]}
    ${avro_serializer}=  Get Avro Serializer  ${schema_str}  ${schema_registry_client}
    ${avro_deserializer}=  Get Avro Deserializer  ${schema_str}  ${schema_registry_client}
    ${string_serializer}=  Get String Serializer
    ${string_deserializer}=  Get String Deserializer

    ${producer_id}=  Create Producer  key_serializer=${string_serializer}  value_serializer=${avro_serializer}  serializing=${True}
    ${value}=  Create Dictionary  name=Robot  number=${10}
    Produce  group_id=${producer_id}  topic=avro_testing1  partition=${0}  value=${value}  key=${KEY}
    Wait Until Keyword Succeeds  10x  0.5s  All Messages Are Delivered  ${producer_id}

    ${consumer_group_id}=  Create Consumer  auto_offset_reset=latest  key_deserializer=${string_deserializer}  value_deserializer=${avro_deserializer}  deserializing=${True}
    Subscribe Topic  group_id=${consumer_group_id}  topics=avro_testing1
    Poll  group_id=${consumer_group_id}  # Dummy poll when using offset reset 'latest'

    Produce  group_id=${producer_id}  topic=avro_testing1  value=${value}  partition=${0}  key=${KEY}
    Wait Until Keyword Succeeds  10x  0.5s  All Messages Are Delivered  ${producer_id}
    ${messages}=  Poll  group_id=${consumer_group_id}
    Should Be Equal  ${messages}  ${TEST_DATA}
    [Teardown]  Basic Teardown  ${consumer_group_id}


*** Keywords ***
Starting Test
    Set Suite Variable  @{TEST_TOPIC}  avro_testing1
    Set Suite Variable  ${KEY}  568a68fd-2785-44cc-8997-1295c3755d28

    ${value}=  Create Dictionary  name=Robot  number=${10}
    ${data}=  Create List  ${value}
    Set Suite Variable  ${TEST_DATA}  ${data}

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