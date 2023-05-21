# Robot Framework - ConfluentKafkaLibrary

ConfluentKafkaLibrary library is a wrapper for the [confluent-kafka-python](https://github.com/confluentinc/confluent-kafka-python).

ConfluentKafkaLibrary is compatible with the latest version of confluent-kafka-python, where the library versions have a 1:1 correspondence (e.g., ConfluentKafkaLibrary 1.3.0 corresponds to confluent-kafka-python 1.3.0). Bug fixes and updates are denoted by a trailing hyphen, such as `1.3.0-1`.

## Documentation

The keyword documentation for ConfluentKafkaLibrary can be found [here](https://robooo.github.io/robotframework-ConfluentKafkaLibrary/)

To generate the documentation, use the following command:

```
python -m robot.libdoc -f html src/ConfluentKafkaLibrary docs/index.html
```

## Installation

To install the library, run the following command:

```
pip install robotframework-confluentkafkalibrary
```

## Usage

In most cases, you can refer to the [confluent-kafka-python documentation](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html) for guidance. Every keyword in ConfluentKafkaLibrary is designed to match the corresponding Python functions. If you are unsure about the pre-configured keywords, please visit the  [robotframework-ConfluentKafkaLibrary documentation](https://robooo.github.io/robotframework-ConfluentKafkaLibrary/). The Kafka team maintains the up-to-date documentation for configuration properties and their values [here](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).

* You can find basic usage examples in the [./examples/test.robot](./examples/test.robot)
* For more complex examples, such as handling byte data from a topic, using multiple consumers, or running threaded avro consumers, please refer to the [documentation](https://robooo.github.io/robotframework-ConfluentKafkaLibrary/#Examples).

## Testing

* The library is tested using black-box tests written in Robot Framework.
  * You can find the test files in the [examples/ directory](./examples) directory.
* For testing, a dockerized enterprise Kafka platform with schema registry support and REST proxy is used. The platform is deployed and tested for each pull request and merge to the master branch.
  * See [docker-compose.yml](./examples/docker-compose.yml) file with the necessary configuration.
* Tests are divided into the following files:
  * test.robot - Basic tests to verify functionality of the Consumer and Producer.
  * test_adminclient.robot - Verifications of admin client functionality.
  * test_avro.robot - Verifications of avro and serializers functionality.
* Not executable example of oauth usage can be found [here](https://github.com/robooo/robotframework-ConfluentKafkaLibrary/blob/master/examples/test_oauth.robot#L14)
  * Update of deployment https://github.com/robooo/robotframework-ConfluentKafkaLibrary/issues/21 is required.
* The core testing logic involves producing data to Kafka, connecting one consumer in a thread, and working with the results in specific test cases.

## Known Limitations:
* Unable to install robotframework-confluentkafkalibrary on Amazon EC2 graviton instance type
  * see the [steps to resolve](https://github.com/robooo/robotframework-ConfluentKafkaLibrary/issues/33#issuecomment-1464644752)
