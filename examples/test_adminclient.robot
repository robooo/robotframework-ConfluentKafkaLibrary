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

    ${group_id}=  Create Consumer  auto_offset_reset=earliest
    ${topics}=  List Topics  ${group_id}
    FOR  ${topic}  IN  @{topic_names}
      List Should Contain Value  ${topics}  ${topic}
    END
    Close Consumer  ${group_id}
    [Teardown]  Delete Topics  ${admin_client_id}  ${topic_names}

AdminClient New Partitions
    ${topic_name}=  Set Variable  admin_testing_partition
    ${topic}=  New Topic  ${topic_name}  num_partitions=${1}  replication_factor=${1}
    ${admin_client_id}=  Create Admin Client
    Create Topics  group_id=${admin_client_id}  new_topics=${topic}

    ${new_parts}=  New Partitions  ${topic_name}  new_total_count=${2}
    Create Partitions  group_id=${admin_client_id}  new_partitions=${new_parts}
    [Teardown]  Delete Topics  ${admin_client_id}  ${topic_name}
