# Robot Framework - ConfluentKafkaLibrary

[![Build Status](https://travis-ci.org/robooo/robotframework-ConfluentKafkaLibrary.svg?branch=master)](https://travis-ci.org/robooo/robotframework-ConfluentKafkaLibrary)

ConfluentKafkaLibrary library is wrapper for [confluent-kafka-python](https://github.com/confluentinc/confluent-kafka-python).

Still in development, right now supports:

* [ ] Consumer
  * [X] Poll
  * [X] Un/Subscribe
  * [X] Create / Stop consumer
  * [X] Assign
  * [X] List topics
  * [ ] commit
  * [ ] offsets
  * [X] Run in thread
  * [X] Decode option of data from topic
* [X] Producer


ConfluentKafkaLibrary works with latest confluent-kafka-python 1.0.0.


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

## Usage Examples

See https://robooo.github.io/robotframework-ConfluentKafkaLibrary/#Examples

