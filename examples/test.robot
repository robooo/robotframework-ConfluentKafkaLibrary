*** Settings ***
Library  ConfluentKafkaLibrary


*** Test Cases ***
My Test
    ${group_id}=  Create Consumer  group_id=my_id
    Log To Console  ${group_id}
