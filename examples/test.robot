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
    ${messages}=  Poll  group_id=${group_id}  max_records=3  decode_format=utf8
    ${data}=  Create List  Hello  World  {'test': 1}
    Lists Should Be Equal  ${messages}  ${data}
    Unsubscribe  ${group_id}
    Close Consumer  ${group_id}

Consumer With Assignment
    ${group_id}=  Create Consumer
    ${topics}=  List Topics  ${group_id}
    ${partitions}=  Get Topic Partitions  ${topics['${TEST_TOPIC}']}
    ${partition_id}=  Set Variable  ${partitions[0].id}
    ${tp}=  Create Topic Partition  ${TEST_TOPIC}  ${P_ID}  ${OFFSET_END}
    Assign To Topic Partition  ${group_id}  ${tp}
    Sleep  1sec  # Need wait for assignment
    Prepare Data
    ${messages}=  Poll  group_id=${group_id}  max_records=6  decode_format=utf8
    Lists Should Be Equal  ${TEST_DATA}  ${messages}
    Unsubscribe  ${group_id}
    Close Consumer  ${group_id}

Verify Test And Threaded Consumer
    [Setup]  Clear Messages From Thread  ${MAIN_THREAD}
    ${group_id}=  Create Consumer
    Subscribe Topic  group_id=${group_id}  topics=${TEST_TOPIC}
    ${messages}=  Poll  group_id=${group_id}
    Prepare Data
    ${thread_messages}=  Get Messages From Thread  ${MAIN_THREAD}  decode_format=utf-8
    ${messages}=  Poll  group_id=${group_id}  max_records=6  decode_format=utf8
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
    ${thread_messages1}=  Get Messages From Thread  ${MAIN_THREAD}  decode_format=utf-8
    Clear Messages From Thread  ${MAIN_THREAD}
    Produce  group_id=${PRODUCER_ID}  topic=${TEST_TOPIC}  value=After  partition=${P_ID}
    Produce  group_id=${PRODUCER_ID}  topic=${TEST_TOPIC}  value=Clear  partition=${P_ID}
    Wait Until Keyword Succeeds  10x  0.5s  All Messages Are Delivered  ${PRODUCER_ID}
    Sleep  1sec  # if next command is polling messages in thread we need to wait a second

    ${thread_messages2}=  Get Messages From Thread  ${MAIN_THREAD}  decode_format=utf-8
    ${data}=  Create List  After  Clear
    Should Be Equal  ${data}  ${thread_messages2}

    Produce  group_id=${PRODUCER_ID}  topic=${TEST_TOPIC}  value=LAST  partition=${P_ID}
    Wait Until Keyword Succeeds  10x  0.5s  All Messages Are Delivered  ${PRODUCER_ID}
    Sleep  1sec
    Append To List  ${data}  LAST
    ${thread_messages2}=  Get Messages From Thread  ${MAIN_THREAD}  decode_format=utf-8
    Should Be Equal  ${TEST_DATA}  ${thread_messages1}
    Should Be Equal  ${data}  ${thread_messages2}
    [Teardown]  Clear Messages From Thread  ${MAIN_THREAD}


*** Keywords ***
Starting Test
    Set Suite Variable  ${TEST_TOPIC}  test
    # we set offset to latest if there was some test run before
    ${thread}=  Start Consumer Threaded  topics=${TEST_TOPIC}  auto_offset_reset=latest
    Set Suite Variable  ${MAIN_THREAD}  ${thread}
    ${producer_group_id}=  Create Producer
    Set Suite Variable  ${PRODUCER_ID}  ${producer_group_id}

    ${topics}=  List Topics  ${producer_group_id}
    ${partitions}=  Get Topic Partitions  ${topics['${TEST_TOPIC}']}
    ${partition_id}=  Set Variable  ${partitions[0].id}
    Set Suite Variable  ${P_ID}  ${partition_id}
    ${tp}=  Create Topic Partition  ${TEST_TOPIC}  ${partition_id}  ${OFFSET_BEGINNING}

    ${data}=  Create List  Hello  World  {'test': 1}  {'test': 2}  {'test': 3}  {'test': 4}
    Set Suite Variable  ${TEST_DATA}  ${data}
    Prepare Data

Prepare Data
    Produce  group_id=${PRODUCER_ID}  topic=${TEST_TOPIC}  value=Hello  partition=${P_ID}
    Produce  group_id=${PRODUCER_ID}  topic=${TEST_TOPIC}  value=World  partition=${P_ID}
    Produce  group_id=${PRODUCER_ID}  topic=${TEST_TOPIC}  value={'test': 1}  partition=${P_ID}
    Produce  group_id=${PRODUCER_ID}  topic=${TEST_TOPIC}  value={'test': 2}  partition=${P_ID}
    Produce  group_id=${PRODUCER_ID}  topic=${TEST_TOPIC}  value={'test': 3}  partition=${P_ID}
    Produce  group_id=${PRODUCER_ID}  topic=${TEST_TOPIC}  value={'test': 4}  partition=${P_ID}
    Wait Until Keyword Succeeds  10x  0.5s  All Messages Are Delivered  ${PRODUCER_ID}
    Sleep  1sec  # if next command is polling messages in thread we need to wait a second

All Messages Are Delivered
    [Arguments]  ${producer_id}
    ${count}=  Flush  ${producer_id}
    Log  Reaming messages to be delivered: ${count}
    Should Be Equal As Integers  ${count}  0
