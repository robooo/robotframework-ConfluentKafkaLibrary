*** Settings ***
Library  ConfluentKafkaLibrary
Library  Collections

Suite Setup  Starting Test


*** Test Cases ***
Verify Topics
    ${group_id}=  Create Consumer  auto_offset_reset=earliest
    ${topics}=  List Topics  ${group_id}
    Dictionary Should Contain Key  ${topics}  test
    Close Consumer  ${group_id}

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
    ${xxx}=  Get Messages From Thread  ${MAIN_THREAD}
    ${group_id}=  Create Consumer  auto_offset_reset=earliest
    ${topics}=  List Topics  ${group_id}
    Subscribe Topic  group_id=${group_id}  topics=test
    ${messages}=  Poll  group_id=${group_id}  max_records=3
    # List Should Contain Sub List  ${xxx}  ${messages}
    Unsubscribe  ${group_id}
    Log To Console  ${xxx}
    Clear Messages From Thread  ${MAIN_THREAD}
    ${thread_messages2}=  Get Messages From Thread  ${MAIN_THREAD}
    Log To Console  ${xxx}
    Log To Console  ${thread_messages2}
    Log To Console  ${thread_messages2}
    Log To Console  XXXXXXXXXX
    # ${xxx}=  Set Variable  a
    ${producer_group_id}=  Create Producer
    Produce  group_id=${producer_group_id}  topic=test  value=After
    Produce  group_id=${producer_group_id}  topic=test  value=Change
    Sleep  1sec
    ${thread_messages2}=  Get Messages From Thread  ${MAIN_THREAD}
    Log To Console  ${xxx}
    Log To Console  ${thread_messages2}
    Produce  group_id=${producer_group_id}  topic=test  value=LAST
    Sleep  2sec
    ${thread_messages2}=  Get Messages From Thread  ${MAIN_THREAD}
    Log To Console  ${xxx}
    Log To Console  ${thread_messages2}
    Log To Console  XXXXXXXXXX

    Close Consumer  ${group_id}

*** Keywords ***
Starting Test
    ${thread}=  Start Consumer Threaded  topics=test  auto_offset_reset=latest
    Set Suite Variable  ${MAIN_THREAD}  ${thread}

    ${producer_group_id}=  Create Producer
    Produce  group_id=${producer_group_id}  topic=test  value=Hello
    Produce  group_id=${producer_group_id}  topic=test  value=World
    Produce  group_id=${producer_group_id}  topic=test  value={'test': 1}
    ${topics}=  List Topics  ${producer_group_id}
    Should Not Be Empty  ${topics}
    Flush  ${producer_group_id}
