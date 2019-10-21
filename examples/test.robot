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
    ${thread_messages}=  Get Messages From Thread  ${MAIN_THREAD}
    Log To Console  Thread messages: ${thread_messages}
    ${group_id}=  Create Consumer  auto_offset_reset=earliest
    Subscribe Topic  group_id=${group_id}  topics=test
    ${messages}=  Poll  group_id=${group_id}  max_records=3
    # List Should Contain Sub List  ${thread_messages}  ${messages}
    Unsubscribe  ${group_id}
    Clear Messages From Thread  ${MAIN_THREAD}

    ${thread_messages2}=  Get Messages From Thread  ${MAIN_THREAD}
    # List Should Contain Sub List  ${thread_messages}  ${messages}
    Log To Console  Thread messages: ${thread_messages}
    Log To Console  Thread2 messages: ${thread_messages2}
    Log To Console  Thread2 messages: ${thread_messages2}
    Log To Console  XXXXXXXXXX


    ${producer_group_id}=  Create Producer
    Produce  group_id=${producer_group_id}  topic=test  value=After
    Produce  group_id=${producer_group_id}  topic=test  value=Change
    Flush  ${producer_group_id}
    ${thread_messages2}=  Get Messages From Thread  ${MAIN_THREAD}
    Log To Console  Thread messages: ${thread_messages}
    Log To Console  Thread2 messages: ${thread_messages2}
    Produce  group_id=${producer_group_id}  topic=test  value=LAST
    Flush  ${producer_group_id}
    ${thread_messages2}=  Get Messages From Thread  ${MAIN_THREAD}
    Log To Console  Thread messages: ${thread_messages}
    Log To Console  Thread2 messages: ${thread_messages2}
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
