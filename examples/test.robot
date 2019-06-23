*** Settings ***
Library  ConfluentKafkaLibrary
Library  Collections

Suite Setup  Starting Test
Suite Teardown  Stop Thread  ${MAIN_THREAD}


*** Test Cases ***
Basic Consumer
    ${group_id}=  Create Consumer  auto_offset_reset=earliest
    Subscribe Topic  group_id=${group_id}  topics=test
    ${messages}=  Poll  group_id=${group_id}  max_records=3
    ${messages}=  Decode Data  ${messages}  decode_format=utf8
    ${data}=  Create List  Hello  World  {'test': 1}
    Lists Should Be Equal  ${messages}  ${data}
    Unsubscribe  ${group_id}
    Close Consumer  ${group_id}

Verify Threaded Consumer
    ${thread_messages}=  Get Messages From Thread  ${MAIN_THREAD}
    ${group_id}=  Create Consumer  auto_offset_reset=earliest
    Subscribe Topic  group_id=${group_id}  topics=test
    ${messages}=  Poll  group_id=${group_id}  max_records=3
    List Should Contain Sub List  ${thread_messages}  ${messages}
    Unsubscribe  ${group_id}
    Close Consumer  ${group_id}

*** Keywords ***
Starting Test
    ${thread}=  Start Consumer Threaded  topics=test  auto_offset_reset=earliest
    Set Global Variable  ${MAIN_THREAD}  ${thread}

    ${producer_group_id}=  Create Producer
    Produce  group_id=${producer_group_id}  topic=test  value=Hello
    Produce  group_id=${producer_group_id}  topic=test  value=World
    Produce  group_id=${producer_group_id}  topic=test  value={'test': 1}
    Flush  ${producer_group_id}
