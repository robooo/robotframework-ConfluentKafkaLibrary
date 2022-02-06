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
    Create Topics  group_id=${admin_client_id}  new_topics=${topics}
    ${topics}=  List Topics  ${admin_client_id}
    FOR  ${topic}  IN  @{topic_names}
      List Should Contain Value  ${topics}  ${topic}
    END
    [Teardown]  Delete Topics  ${admin_client_id}  ${topic_names}

AdminClient List Groups
    [Documentation]  If you run this test as first switch to Should Be Empty keyword.
    ${admin_client_id}=  Create Admin Client
    ${groups}=  List Groups  ${admin_client_id}
    Should Not Be Empty  ${groups}

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
