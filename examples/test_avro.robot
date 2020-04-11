*** Settings ***
Library  ConfluentKafkaLibrary
Library  Collections
Library  String

Suite Setup  Starting Test


*** Test Cases ***
Avro Producer With Schemas As String Argument
    [Setup]  Clear Messages From Thread  ${MAIN_THREAD}
    ${value_schema}=  Set Variable  {"namespace": "example.avro","type": "record","name": "User","fields": [{"name": "name","type": "string"},{"name": "number","type": ["int","null"]}]}
    ${key_schema}=  Set Variable  {"namespace": "example.avro","type": "record","name": "User","fields": [{"name": "name","type": "string"}]}
    ${producer_id}=  Create Producer  schema_registry_url=http://127.0.0.1:8081
    ...  value_schema=${value_schema}  key_schema=${key_schema}
    ${value}=  Create Dictionary  name=Robot  number=${10}
    Produce  group_id=${producer_id}  topic=avro_testing1  partition=${0}  value=${value}  key=${KEY}
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
    ${value_schema_file_path}=  Set Variable  examples/schema/producer/ValueSchema.avsc
    ${key_schema_file_path}=  Set Variable  examples/schema/producer/KeySchema.avsc
    ${producer_id}=  Create Producer  schema_registry_url=http://127.0.0.1:8081
    ...  value_schema=${value_schema_file_path}  key_schema=${key_schema_file_path}
    ${value}=  Create Dictionary  name=Robot  number=${10}
    Produce  group_id=${producer_id}  topic=avro_testing2  partition=${0}  value=${value}  key=${KEY}
    Wait Until Keyword Succeeds  10x  0.5s  All Messages Are Delivered  ${producer_id}
    Sleep  1s

    ${consumer_group_id}=  Create Consumer  auto_offset_reset=earliest  schema_registry_url=http://127.0.0.1:8081
    Subscribe Topic  group_id=${consumer_group_id}  topics=avro_testing2
    ${messages}=  Poll  group_id=${consumer_group_id}
    Should Be Equal  ${TEST_DATA}  ${messages}
    ${thread_messages}=  Get Messages From Thread  ${MAIN_THREAD}
    Should Be Equal  ${TEST_DATA}  ${thread_messages}
    [Teardown]  Basic Teardown  ${consumer_group_id}


*** Keywords ***
Starting Test
    Set Suite Variable  @{TEST_TOPIC}  avro_testing1  avro_testing2
    Set Suite Variable  &{KEY}  name=testkey
    ${value}=  Create Dictionary  name=Robot  number=${10}
    ${data}=  Create List  ${value}
    Set Suite Variable  ${TEST_DATA}  ${data}

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

Unassign Teardown
    [Arguments]  ${group_id}
    Unassign  ${group_id}
    Close Consumer  ${group_id}