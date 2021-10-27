*** Settings ***
Library  ConfluentKafkaLibrary
Library  oauth_example
Library  Collections
Library  String


*** Variables ***
${SEEN_RF_OAUTH_CB}  ${False}


*** Test Cases ***
Example Oauth
    [Documentation]  Example of how to use OAUTH with library and call functools
    ...              via get_token function. For better handling there could be 
    ...              some global variable which can be set inside of python lib.
    ...              Not executable right now, needs update env (issue #21).

    Skip

    ${string_serializer}=  Get String Serializer
    ${value_serializer}=  Get String Serializer

    # This returns: functools.partial(<function oauth_cb at 0x7f...>, 'configuration')
    ${fun}=  oauth_example.get_token  configuration

    ${producer_id}=  Create Producer  key_serializer=${string_serializer}  value_serializer=${value_serializer}  legacy=${False}  security.protocol=sasl_plaintext  sasl.mechanisms=OAUTHBEARER  oauth_cb=${fun}

    #...
