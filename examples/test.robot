*** Settings ***
Library  ConfluentKafkaLibrary


*** Test Cases ***
My Test
    ${group_id}=  Create Consumer  group_id=my_id
    Log To Console  ${group_id}

My Test Threaded
    ${thread}=  Start Messages Threaded  topics=foobar
    ${messages}=  Get Messages Threaded  ${thread}
    Log To Console  ${messages}
    Sleep  20s
    ${messages}=  Get Messages Threaded  ${thread}
    Log To Console  ${messages}
    Stop Thread  ${thread}