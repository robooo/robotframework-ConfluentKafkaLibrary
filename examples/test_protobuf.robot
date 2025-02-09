*** Settings ***
Library  ConfluentKafkaLibrary
Library  schema/protobuf/user_helper.py

*** Test Cases ***
Protobuf Producer With Serializer
    ${schema_registry_conf}=  Create Dictionary  url=http://127.0.0.1:8081
    ${schema_registry_client}=  Get Schema Registry Client  ${schema_registry_conf}
    ${msg_type}=  Get Type
    ${protobuf_serializer}=  Get Protobuf Serializer  ${msg_type}  ${schema_registry_client}
    ${protobuf_deserializer}=  Get Protobuf Deserializer  ${msg_type}
    ${string_serializer}=  Get String Serializer

    ${producer_id}=  Create Producer  key_serializer=${string_serializer}  value_serializer=${protobuf_serializer}  serializing=${True}
    ${value}=  Create User  Robot  10
    Produce  group_id=${producer_id}  topic=protobuf_testing1  key=bd232464-e3d3-425d-93b7-5789dc7273c1  value=${value}
    Wait Until Keyword Succeeds  10x  0.5s  All Messages Are Delivered  ${producer_id}

Protobuf Producer Consumer With Serializer
    ${schema_registry_conf}=  Create Dictionary  url=http://127.0.0.1:8081
    ${schema_registry_client}=  Get Schema Registry Client  ${schema_registry_conf}
    ${msg_type}=  Get Type
    ${protobuf_serializer}=  Get Protobuf Serializer  ${msg_type}  ${schema_registry_client}
    ${protobuf_deserializer}=  Get Protobuf Deserializer  ${msg_type}
    ${string_serializer}=  Get String Serializer
    ${string_deserializer}=  Get String Deserializer

    ${producer_id}=  Create Producer  key_serializer=${string_serializer}  value_serializer=${protobuf_serializer}  serializing=${True}
    ${value}=  Create User  Robot  10
    Produce  group_id=${producer_id}  topic=protobuf_testing2  key=f01df0c6-ec0b-49e9-835f-d766a9e8036f  value=${value}
    Wait Until Keyword Succeeds  10x  0.5s  All Messages Are Delivered  ${producer_id}

    ${consumer_group_id}=  Create Consumer  auto_offset_reset=earliest  key_deserializer=${string_deserializer}  value_deserializer=${protobuf_deserializer}  deserializing=${True}
    Subscribe Topic  group_id=${consumer_group_id}  topics=protobuf_testing2
    ${messages}=  Poll  group_id=${consumer_group_id}
    Length Should Be  ${messages}  1
    Should Be Equal  ${messages[0]}  ${value}
    [Teardown]  Basic Teardown  ${consumer_group_id}

*** Keywords ***
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
