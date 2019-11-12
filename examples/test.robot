*** Settings ***
Library  ConfluentKafkaLibrary
Library  Collections

Suite Setup  Starting Test


*** Test Cases ***
Verify Topics
    ${group_id}=  Create Consumer  auto_offset_reset=earliest
    ${topics}=  List Topics  ${group_id}
    Dictionary Should Contain Key  ${topics}  ${TEST_TOPIC}
    Close Consumer  ${group_id}

Basic Consumer
    ${group_id}=  Create Consumer  auto_offset_reset=earliest
    Subscribe Topic  group_id=${group_id}  topics=${TEST_TOPIC}
    ${messages}=  Poll  group_id=${group_id}  max_records=3  only_value=${True}  decode_format=utf8
    ${data}=  Create List  Hello  World  {'test': 1}
    Lists Should Be Equal  ${messages}  ${data}
    Unsubscribe  ${group_id}
    Close Consumer  ${group_id}

Consumer With Assignment
    ${group_id}=  Create Consumer
    ${topics}=  List Topics  ${group_id}
    ${partitions}=  Get Topic Partitions  ${topics['${TEST_TOPIC}']}
    Log To Console  ${partitions[0].id}
    ${partition_id}=  Set Variable  ${partitions[0].id}
    ${tp}=  Create Topic Partition  ${TEST_TOPIC}  ${partition_id}  ${OFFSET_END}
    Assign To Topic Partition  ${group_id}  ${tp}
    Prepare Data
    ${messages}=  Poll  group_id=${group_id}  max_records=6  only_value=${True}  decode_format=utf8
    Lists Should Be Equal  ${TEST_DATA}  ${messages}
    Unsubscribe  ${group_id}
    Close Consumer  ${group_id}

Verify Test And Threaded Consumer
    [Setup]  Clear Messages From Thread  ${MAIN_THREAD}
    ${group_id}=  Create Consumer
    Subscribe Topic  group_id=${group_id}  topics=${TEST_TOPIC}
    ${messages}=  Poll  group_id=${group_id}  only_value=${True}
    Prepare Data
    ${thread_messages}=  Get Messages From Thread  ${MAIN_THREAD}  decode_format=utf-8
    ${messages}=  Poll  group_id=${group_id}  max_records=6  only_value=${True}  decode_format=utf8
    Lists Should Be Equal  ${thread_messages}  ${messages}
    Unsubscribe  ${group_id}
    Close Consumer  ${group_id}
    [Teardown]  Clear Messages From Thread  ${MAIN_THREAD}

Verify Clean Of Threaded Consumer Messages
    [Setup]  Prepare Data
    ${thread_messages1}=  Get Messages From Thread  ${MAIN_THREAD}  decode_format=utf-8
    Clear Messages From Thread  ${MAIN_THREAD}
    ${thread_messages2}=  Get Messages From Thread  ${MAIN_THREAD}
    Lists Should Be Equal  ${TEST_DATA}  ${thread_messages1}
    Should Be Empty  ${thread_messages2}
    [Teardown]  Clear Messages From Thread  ${MAIN_THREAD}

Remove And Publish New Messages From Threaded Consumer
    [Setup]  Prepare Data
    ${thread_messages1}=  Get Messages From Thread  ${MAIN_THREAD}  #decode_format=utf-8
    Clear Messages From Thread  ${MAIN_THREAD}

    ${producer_group_id}=  Create Producer
    Produce  group_id=${producer_group_id}  topic=${TEST_TOPIC}  value=After
    Produce  group_id=${producer_group_id}  topic=${TEST_TOPIC}  value=Change
    Flush  ${producer_group_id}
    All Messages Are Delivered  ${producer_group_id}
    ${thread_messages2}=  Get Messages From Thread  ${MAIN_THREAD}
    Log To Console  Thread2 messages: ${thread_messages2}
    Produce  group_id=${producer_group_id}  topic=${TEST_TOPIC}  value=LAST
    Flush  ${producer_group_id}
    All Messages Are Delivered  ${producer_group_id}
    ${thread_messages2}=  Get Messages From Thread  ${MAIN_THREAD}
    Log To Console  Thread1 messages: ${thread_messages1}
    Log To Console  Thread2 messages: ${thread_messages2}
    [Teardown]  Clear Messages From Thread  ${MAIN_THREAD}


*** Keywords ***
Starting Test
    Set Suite Variable  ${TEST_TOPIC}  test
    ${thread}=  Start Consumer Threaded  topics=${TEST_TOPIC}  only_value=${True}  auto_offset_reset=latest
    Set Suite Variable  ${MAIN_THREAD}  ${thread}
    ${producer_group_id}=  Create Producer
    Set Suite Variable  ${PRODUCER_ID}  ${producer_group_id}
    ${data}=  Create List  Hello  World  {'test': 1}  {'test': 2}  {'test': 3}  {'test': 4}
    Set Suite Variable  ${TEST_DATA}  ${data}

Prepare Data
    Produce  group_id=${PRODUCER_ID}  topic=${TEST_TOPIC}  value=Hello
    Produce  group_id=${PRODUCER_ID}  topic=${TEST_TOPIC}  value=World
    Produce  group_id=${PRODUCER_ID}  topic=${TEST_TOPIC}  value={'test': 1}
    Produce  group_id=${PRODUCER_ID}  topic=${TEST_TOPIC}  value={'test': 2}
    Produce  group_id=${PRODUCER_ID}  topic=${TEST_TOPIC}  value={'test': 3}
    Produce  group_id=${PRODUCER_ID}  topic=${TEST_TOPIC}  value={'test': 4}
    Wait Until Keyword Succeeds  10x  0.5s  All Messages Are Delivered  ${PRODUCER_ID}
    Sleep  1sec  # if next command is polling messages in thread we need to wait a second

All Messages Are Delivered
    [Arguments]  ${producer_id}
    ${count}=  Flush  ${producer_id}
    Log To Console  Reaming: ${count}
    Should Be Equal As Integers  ${count}  0
