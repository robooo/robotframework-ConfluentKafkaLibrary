*** Settings ***
Library  ConfluentKafkaLibrary
Library  Collections

Suite Setup  Starting Test
Suite Teardown  Stop Thread

*** Test Cases ***
Verify Topics
    ${group_id}=  Create Consumer  auto_offset_reset=earliest
    ${topics}=  List Topics  ${group_id}
    Dictionary Should Contain Key  ${topics}  ${TEST_TOPIC}
    [Teardown]  Close Consumer  ${group_id}

Basic Consumer
    ${group_id}=  Create Consumer  auto_offset_reset=earliest
    Subscribe Topic  group_id=${group_id}  topics=${TEST_TOPIC}
    ${messages}=  Poll  group_id=${group_id}  max_records=3  decode_format=utf8
    ${data}=  Create List  Hello  World  {'test': 1}
    Lists Should Be Equal  ${messages}  ${data}
    [Teardown]  Basic Teardown  ${group_id}

Produce Without Value
    ${topic_name}=  Set Variable  topicwithoutvaluee
    Produce  group_id=${PRODUCER_ID}  topic=${topic_name}
    Wait Until Keyword Succeeds  10x  0.5s  All Messages Are Delivered  ${PRODUCER_ID}
    ${group_id}=  Create Consumer  auto_offset_reset=earliest
    Subscribe Topic  group_id=${group_id}  topics=${topic_name}
    ${messages}=  Poll  group_id=${group_id}  max_records=1
    Should Be Equal As Strings  ${messages}  [None]
    [Teardown]  Basic Teardown  ${group_id}

Verify Position
    ${group_id}=  Create Consumer
    ${tp}=  Create Topic Partition  ${TEST_TOPIC}  ${P_ID}  ${OFFSET_END}
    Assign To Topic Partition  ${group_id}  ${tp}
    Sleep  5sec  # Need to wait for an assignment
    ${position}=  Get Position  group_id=${group_id}  topic_partitions=${tp}
    ${position_before}=  Set Variable  ${position[0].offset}

    Produce  group_id=${PRODUCER_ID}  topic=${TEST_TOPIC}  value=Dummy  partition=${P_ID}
    Wait Until Keyword Succeeds  10x  0.5s  All Messages Are Delivered  ${PRODUCER_ID}
    ${position}=  Get Position  group_id=${group_id}  topic_partitions=${tp}
    ${position_after_produce}=  Set Variable  ${position[0].offset}
    Should Be Equal As Integers  ${position_before}  ${position_after_produce}

    ${messages}=  Poll  group_id=${group_id}  max_records=1  decode_format=utf8
    ${position}=  Get Position  group_id=${group_id}  topic_partitions=${tp}
    ${position_after_poll_1}=  Set Variable  ${position[0].offset}
    Should Not Be Equal As Integers  ${position_after_poll_1}  ${position_after_produce}

    Produce  group_id=${PRODUCER_ID}  topic=${TEST_TOPIC}  value=Dummy  partition=${P_ID}
    Wait Until Keyword Succeeds  10x  0.5s  All Messages Are Delivered  ${PRODUCER_ID}
    ${messages}=  Poll  group_id=${group_id}  max_records=1  decode_format=utf8
    ${position}=  Get Position  group_id=${group_id}  topic_partitions=${tp}
    ${position_after_poll_2}=  Set Variable  ${position[0].offset}
    Should Be Equal As Integers  ${position_after_poll_1 + 1}  ${position_after_poll_2}
    [Teardown]  Basic Teardown  ${group_id}

Consumer With Assignment To Last Message After Get Of Watermark Offsets
    ${group_id}=  Create Consumer
    ${tp}=  Create Topic Partition  ${TEST_TOPIC}  ${P_ID}
    ${offset}=  Get Watermark Offsets  ${group_id}  ${tp}
    ${tp}=  Create Topic Partition  ${TEST_TOPIC}  ${P_ID}  ${offset[1]}
    Assign To Topic Partition  ${group_id}  ${tp}
    Prepare Data
    ${messages}=  Poll  group_id=${group_id}  max_records=6  decode_format=utf8
    Lists Should Be Equal  ${TEST_DATA}  ${messages}
    [Teardown]  Basic Teardown  ${group_id}

Consumer With Assignment To OFFSET_END
    ${group_id}=  Create Consumer
    ${tp}=  Create Topic Partition  ${TEST_TOPIC}  ${P_ID}  ${OFFSET_END}
    Assign To Topic Partition  ${group_id}  ${tp}
    # Need to wait for an async assignment, be aware the Is Assigned could return True but
    # that doesn't mean assignment is completed
    Sleep  5sec
    Prepare Data
    ${messages}=  Poll  group_id=${group_id}  poll_attempts=30  max_records=6  timeout=5  decode_format=utf8
    Lists Should Be Equal  ${TEST_DATA}  ${messages}
    [Teardown]  Unassign Teardown  ${group_id}

Verify Test And Threaded Consumer
    [Setup]  Clear Messages From Thread  ${MAIN_THREAD}
    ${group_id}=  Create Consumer
    Subscribe Topic  group_id=${group_id}  topics=${TEST_TOPIC}
    ${messages}=  Poll  group_id=${group_id}
    Prepare Data
    ${thread_messages}=  Get Messages From Thread  ${MAIN_THREAD}  decode_format=utf-8
    ${messages}=  Poll  group_id=${group_id}  max_records=6  decode_format=utf8
    Lists Should Be Equal  ${thread_messages}  ${messages}
    [Teardown]  Run Keywords  Basic Teardown  ${group_id}  AND
                ...  Clear Messages From Thread  ${MAIN_THREAD}

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

Purge Test
    ${producer_id}=  Create Producer  message.timeout.ms=${30000}
    Produce  group_id=${producer_id}  topic=${TEST_TOPIC}  value=After  partition=${P_ID}
    Produce  group_id=${producer_id}  topic=${TEST_TOPIC}  value=Clear  partition=${P_ID}

    Purge  group_id=${producer_id}  in_queue=${False}
    ${count}=  Flush  ${producer_id}  timeout=${0}
    Should Be Equal As Integers  2  ${count}
    Purge  group_id=${producer_id}
    ${count}=  Flush  ${producer_id}  timeout=${0}
    Should Be Equal As Integers  0  ${count}

Offsets Test
    ${group_id}=  Create Consumer  enable.auto.offset.store=${False}
    Subscribe Topic  group_id=${group_id}  topics=${TEST_TOPIC}
    ${tp}=  Create Topic Partition  ${TEST_TOPIC}  ${P_ID}  ${OFFSET_BEGINNING}
    ${offsets}=  Create List  ${tp}
    Run Keyword And Expect Error  KafkaException: *  Store Offsets  group_id=${group_id}  offsets=${offsets}
    Assign To Topic Partition  ${group_id}  ${tp}
    Sleep  5sec
    Store Offsets  group_id=${group_id}  offsets=${offsets}
    [Teardown]  Unassign Teardown  ${group_id}

*** Keywords ***
Starting Test
    Set Suite Variable  ${TEST_TOPIC}  test
    ${thread}=  Start Consumer Threaded  topics=${TEST_TOPIC}
    ${gid}=  Get Thread Group Id  ${thread}
    Log  ${gid}
    Set Suite Variable  ${MAIN_THREAD}  ${thread}
    ${producer_group_id}=  Create Producer
    Set Suite Variable  ${PRODUCER_ID}  ${producer_group_id}

    Set Suite Variable  ${P_ID}  ${0}
    Prepare Data

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

Basic Teardown
    [Arguments]  ${group_id}
    Unsubscribe  ${group_id}
    Close Consumer  ${group_id}
    ${groups}=  Create List  ${group_id}
    ${admin_client_id}=  Create Admin Client
    ${resp}=  Delete Groups  ${admin_client_id}  group_ids=${groups}
    Log  ${resp}

Unassign Teardown
    [Arguments]  ${group_id}
    Unassign  ${group_id}
    Close Consumer  ${group_id}
    ${groups}=  Create List  ${group_id}
    ${admin_client_id}=  Create Admin Client
    ${resp}=  Delete Groups  ${admin_client_id}  group_ids=${groups}
    Log  ${resp}

Stop Thread
    ${resp}=  Stop Consumer Threaded  ${MAIN_THREAD}
    Log  ${resp}