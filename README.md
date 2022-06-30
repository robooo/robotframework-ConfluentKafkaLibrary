# Robot Framework - ConfluentKafkaLibrary

ConfluentKafkaLibrary library is wrapper for [confluent-kafka-python](https://github.com/confluentinc/confluent-kafka-python).

ConfluentKafkaLibrary works with latest confluent-kafka-python, tags are 1:1 (ConfluentKafkaLibrary 1.3.0 == confluent-kafka-python 1.3.0 ). Bugfixes and updates are set after the '-' e.g. `1.3.0-1`.

## Documentation

Keyword documentation is available [here](https://robooo.github.io/robotframework-ConfluentKafkaLibrary/)

How to generate documentation:

```
python -m robot.libdoc -f html src/ConfluentKafkaLibrary docs/index.html
```

## Installation

```
pip install robotframework-confluentkafkalibrary
```

## Usage

In most cases [confluent-kafka-python documentation](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html) is your friend. Every keyword should match the same API as python functions, if you are not sure which are pre-configured please check our [robotframework-ConfluentKafkaLibrary documentation](https://robooo.github.io/robotframework-ConfluentKafkaLibrary/). Up to date documentation of configuration properties and its values is maintained [here](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md) by kafka team.

* Find basic usage examples at [./examples/test.robot](./examples/test.robot)
* More complex examples (handle byte data from topic, use more consumers, run avro consumer threaded) are at [documentation](https://robooo.github.io/robotframework-ConfluentKafkaLibrary/#Examples) too.

## Testing

* This library is tested by black-box tests written in RobotFramework and tests could be found at
  * [examples/ directory](./examples).
* Kafka platform used for testing is dockerized enterprise kafka with schema registry support and rest proxy, deployed & tested on each PR and merge to master.
  * [docker-compose.yml](./examples/docker-compose.yml)
* Output of latest run can be found at https://robooo.github.io/robotframework-ConfluentKafkaLibrary/log.html
* Tests are divided to:
  * test.robot - Basic tests to verify functionality of Consumer / Producer.
  * test_adminclient.robot - Verifications of admin client functionality.
  * test_avro.robot - Verifications of avro and serializers functionality.
* Not executable example of oauth usage can be found [here](https://github.com/robooo/robotframework-ConfluentKafkaLibrary/blob/1.9.0/examples/test_oauth.robot#L14)
  * Update of deployment https://github.com/robooo/robotframework-ConfluentKafkaLibrary/issues/21 is required.
* Core of the testing logic is to produce data to kafka, connect one consumer in thread and work with the results in specific test cases.
