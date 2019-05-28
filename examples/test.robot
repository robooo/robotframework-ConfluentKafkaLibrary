*** Settings ***
Library  ConfluentKafkaLibrary

Suite Setup  Starting Test


*** Test Cases ***
My Test
    ${group_id}=  Create Consumer  group_id=my_id
    Log To Console  ${MAIN_THREAD}
    Get Kafka Messages Key  ${MAIN_THREAD}

My Test New
    Sleep  5sec
    Log To Console  ${MAIN_THREAD}
    Get Kafka Messages Key  ${MAIN_THREAD}

My Test Threaded
    ${thread}=  Start Messages Threaded  topics=test
    ${messages}=  Get Messages From Thread  ${thread}
    Log To Console  ${messages}
    Sleep  10s
    ${messages}=  Get Messages From Thread  ${thread}
    Log To Console  ${messages}
    Get Kafka Messages Key  ${MAIN_THREAD}


*** Keywords ***
Starting Test
    ${thread}=  Start Messages Threaded  topics=foobar
    Set Global Variable  ${MAIN_THREAD}  ${thread}

Get Kafka Messages Key
    [Arguments]  ${thread}
    ${messages}=  Get Messages From Thread  ${thread}  decode_format=utf8  remove_zero_bytes=True
    Log To Console  ${messages}