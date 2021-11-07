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
