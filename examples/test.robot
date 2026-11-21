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

    ${topics_thread}=  List Topics  ${THREADED_GROUPID}
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

Threaded Consumer Keywords Are Executed Without Blocking
    ${start}=  Evaluate  time.monotonic()  modules=time
    ${assignment}=  Get Assignment  ${THREADED_GROUPID}
    ${elapsed}=  Evaluate  time.monotonic() - ${start}  modules=time
    Should Not Be Empty  ${assignment}
    Should Be True  ${elapsed} < 1  Get Assignment took ${elapsed}s

Threaded Consumer Keeps Only Max Messages
    ${topic}=  Evaluate  'threaded-max-' + str(uuid.uuid4())  modules=uuid
    FOR  ${value}  IN  first  second  third  # producing first creates the topic
      Produce  group_id=${PRODUCER_ID}  topic=${topic}  value=${value}  partition=${0}
    END
    Wait Until Keyword Succeeds  10x  0.5s  All Messages Are Delivered  ${PRODUCER_ID}
    ${thread}=  Start Consumer Threaded  topics=${topic}  auto_offset_reset=earliest  max_messages=${2}
    ${expected}=  Create List  second  third
    Wait Until Keyword Succeeds  20x  0.5s  Thread Messages Should Be  ${thread}  ${expected}
    [Teardown]  Stop Consumer Threaded  ${thread}

Close Consumer Stops Threaded Consumer
    ${thread}=  Start Consumer Threaded  topics=${TEST_TOPIC}
    ${group_id}=  Get Thread Group Id  ${thread}
    Close Consumer  ${group_id}
    Wait Until Keyword Succeeds  10x  0.5s  Thread Should Be Stopped  ${thread}
    Run Keyword And Expect Error  ValueError: *  List Topics  ${group_id}
    ${messages}=  Get Messages From Thread  ${thread}
    ${resp}=  Stop Consumer Threaded  ${thread}
    Log  ${resp}

Threaded Consumer Reports Startup Error
    Run Keyword And Expect Error  Threaded consumer * failed: *Invalid value*auto.offset.reset*
    ...  Start Consumer Threaded  topics=${TEST_TOPIC}  auto_offset_reset=invalid

Stop Threaded Consumer Started With Consumer Arguments
    # Create Consumer arguments (auto_offset_reset, ...) must not be passed to the AdminClient
    # which deletes the consumer group, otherwise librdkafka rejects them as unknown properties.
    ${thread}=  Start Consumer Threaded  topics=${TEST_TOPIC}  auto_offset_reset=earliest  enable_auto_commit=${False}
    ${resp}=  Stop Consumer Threaded  ${thread}
    Should Be Equal  ${resp}  ${None}

Purge Test
    # linger.ms keeps the messages in the local queue, so they can't be sent before the first purge
    ${producer_id}=  Create Producer  message.timeout.ms=${30000}  linger.ms=${10000}
    Produce  group_id=${producer_id}  topic=${TEST_TOPIC}  value=After  partition=${P_ID}
    Produce  group_id=${producer_id}  topic=${TEST_TOPIC}  value=Clear  partition=${P_ID}

    Purge  group_id=${producer_id}  in_queue=${False}
    ${count}=  Flush  ${producer_id}  timeout=${0}
    Should Be Equal As Integers  2  ${count}
    Purge  group_id=${producer_id}
    # purged messages leave the queue when their delivery reports are served, flush waits for that
    ${count}=  Flush  ${producer_id}  timeout=${5}
    Should Be Equal As Integers  0  ${count}

Close Producer Delivers Outstanding Messages
    ${topic}=  Evaluate  'closeproducer-' + str(uuid.uuid4())  modules=uuid
    ${producer_id}=  Create Producer
    Produce  group_id=${producer_id}  topic=${topic}  value=Closed  partition=${P_ID}
    ${purged}=  Close Producer  ${producer_id}  # no Flush, close itself delivers the message
    Should Be Equal As Integers  0  ${purged}
    Run Keyword And Expect Error  ValueError: Producer with group_id '${producer_id}' does not exist!
    ...  Close Producer  ${producer_id}

    ${group_id}=  Create Consumer  auto_offset_reset=earliest
    Subscribe Topic  group_id=${group_id}  topics=${topic}
    ${messages}=  Poll  group_id=${group_id}  max_records=1  decode_format=utf8
    ${data}=  Create List  Closed
    Lists Should Be Equal  ${data}  ${messages}
    [Teardown]  Basic Teardown  ${group_id}

Close Producer With Timeout Purges Undelivered Messages
    ${producer_id}=  Create Producer  port=1  # nothing listens there, messages can't be delivered
    Produce  group_id=${producer_id}  topic=${TEST_TOPIC}  value=Lost
    Produce  group_id=${producer_id}  topic=${TEST_TOPIC}  value=Lost
    ${purged}=  Close Producer  ${producer_id}  timeout=${1}
    Should Be Equal As Integers  2  ${purged}

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

Commit And Get Committed Offsets
    # Without auto commit nothing is committed until Commit is called.
    # The committed offset is the next message to consume (last consumed + 1).
    ${topic}=  Evaluate  'commit-' + str(uuid.uuid4())  modules=uuid
    FOR  ${value}  IN  first  second  third
      Produce  group_id=${PRODUCER_ID}  topic=${topic}  value=${value}  partition=${0}
    END
    Wait Until Keyword Succeeds  10x  0.5s  All Messages Are Delivered  ${PRODUCER_ID}
    ${group_id}=  Create Consumer  auto_offset_reset=earliest  enable_auto_commit=${False}
    Subscribe Topic  group_id=${group_id}  topics=${topic}
    ${tp}=  Create Topic Partition  ${topic}  ${0}

    ${messages}=  Poll  group_id=${group_id}  max_records=1  only_value=${False}
    ${committed}=  Get Committed Offsets  ${group_id}  ${tp}  timeout=${10}
    Should Be Equal As Integers  ${committed[0].offset}  ${OFFSET_INVALID}

    Commit  ${group_id}  message=${messages[0]}
    ${committed}=  Get Committed Offsets  ${group_id}  ${tp}  timeout=${10}
    Should Be Equal As Integers  ${committed[0].offset}  1

    ${messages}=  Poll  group_id=${group_id}  max_records=2
    Commit  ${group_id}  # commits the current position
    ${committed}=  Get Committed Offsets  ${group_id}  ${tp}  timeout=${10}
    Should Be Equal As Integers  ${committed[0].offset}  3
    [Teardown]  Basic Teardown  ${group_id}

*** Keywords ***
Starting Test
    Set Suite Variable  ${TEST_TOPIC}  test
    ${thread}=  Start Consumer Threaded  topics=${TEST_TOPIC}
    ${gid}=  Get Thread Group Id  ${thread}
    Log  ${gid}
    Set Suite Variable  ${THREADED_GROUPID}  ${gid}
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

Thread Messages Should Be
    [Arguments]  ${thread}  ${expected}
    ${messages}=  Get Messages From Thread  ${thread}  decode_format=utf-8
    Lists Should Be Equal  ${expected}  ${messages}

Thread Should Be Stopped
    [Arguments]  ${thread}
    ${alive}=  Evaluate  $thread.is_alive()
    Should Not Be True  ${alive}

Stop Thread
    ${resp}=  Stop Consumer Threaded  ${MAIN_THREAD}
    Log  ${resp}