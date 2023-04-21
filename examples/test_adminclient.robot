*** Settings ***
Library  ConfluentKafkaLibrary
Library  Collections


*** Test Cases ***
AdminClient Topic Creation
    ${topic_names}=  Create List  admintesting1  admintesting2  admintesting3
    ${topics}=  Create List
    FOR  ${topic}  IN  @{topic_names}
      ${topic}=  New Topic  ${topic}  num_partitions=${1}  replication_factor=${1}
      Append To List  ${topics}  ${topic}
    END

    ${admin_client_id}=  Create Admin Client
    ${results}=  Create Topics  group_id=${admin_client_id}  new_topics=${topics}
    Log  ${results}
    ${topics}=  List Topics  ${admin_client_id}
    FOR  ${topic}  IN  @{topic_names}
      List Should Contain Value  ${topics}  ${topic}
    END
    [Teardown]  Delete Topics  ${admin_client_id}  ${topic_names}

AdminClient List Consumer Groups
    [Documentation]  If you run this test as first switch to Should Be Empty keyword.
    ${producer_group_id}=  Create Producer
    Produce  ${producer_group_id}  topic=adminlisttest  value=Hello  partition=${0}
    Wait Until Keyword Succeeds  10x  0.5s  All Messages Are Delivered  ${producer_group_id}

    ${group_id}=  Create Consumer  auto_offset_reset=earliest
    Subscribe Topic    ${group_id}    topics=adminlisttest
    Sleep  2s  # Wait for subscription

    ${admin_client_id}=  Create Admin Client
    ${states}=  Create List  ${CONSUMER_GROUP_STATE_UNKNOWN}  #https://github.com/confluentinc/confluent-kafka-python/issues/1556
    ${groups}=  List Groups  ${admin_client_id}  states=${states}
    Log  ${groups}
    Log  ${groups.valid}
    FOR  ${group}  IN  @{groups.valid}
      Log  ${group.group_id}
      IF  "${group_id}" == "${group.group_id}"
        Log  ${group.group_id}
        Log  ${group.state}
        Pass Execution  "Consumer found in list"
      END
    END
    Fail
    [Teardown]  Basic Teardown  ${group_id}

AdminClient New Partitions
    ${topic_name}=  Set Variable  admin_testing_partition
    ${topic}=  New Topic  ${topic_name}  num_partitions=${1}  replication_factor=${1}
    ${admin_client_id}=  Create Admin Client
    Create Topics  group_id=${admin_client_id}  new_topics=${topic}

    ${new_parts}=  New Partitions  ${topic_name}  new_total_count=${2}
    Create Partitions  group_id=${admin_client_id}  new_partitions=${new_parts}
    [Teardown]  Delete Topics  ${admin_client_id}  ${topic_name}

AdminClient Describe Configs
    ${resource}=  Config Resource  ${ADMIN_RESOURCE_BROKER}  1
    ${admin_client_id}=  Create Admin Client
    ${config}=  Describe Configs  ${admin_client_id}  ${resource}

    Should Not Be Empty  ${config}
    ${name}=  Set Variable  ${config['offsets.commit.timeout.ms'].name}
    ${value}=  Set Variable  ${config['offsets.commit.timeout.ms'].value}
    Should Be Equal As Strings  ${name}  offsets.commit.timeout.ms
    Should Be Equal As Integers  ${value}  ${5000}

AdminClient Alter Configs
    ${data}=  Create Dictionary  log.retention.ms=${54321}  # DotDict
    ${data}=  Convert To Dictionary  ${data}                # dict
    ${resource}=  Config Resource  ${ADMIN_RESOURCE_BROKER}  1  set_config=${data}
    ${admin_client_id}=  Create Admin Client

    Alter Configs  ${admin_client_id}  ${resource}
    Sleep  1s
    ${config}=  Describe Configs  ${admin_client_id}  ${resource}
    Should Be Equal As Integers  ${54321}  ${config['log.retention.ms'].value}


*** Keywords ***
All Messages Are Delivered
    [Arguments]  ${producer_id}
    ${count}=  Flush  ${producer_id}
    Log  Reaming messages to be delivered: ${count}
    Should Be Equal As Integers  ${count}  0

Basic Teardown
    [Arguments]  ${group_id}
    Unsubscribe  ${group_id}
    Close Consumer  ${group_id}