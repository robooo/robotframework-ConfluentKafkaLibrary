#!/usr/bin/env python

from os.path import join, dirname
from setuptools import setup

filename=join(dirname(__file__), 'src', 'ConfluentKafkaLibrary', 'version.py')
exec(compile(open(filename).read(),filename, 'exec'))

DESCRIPTION = """
Confluent Kafka wrapped in Robot Framework.
"""[1:-1]

AVRO_REQUIRES = ['fastavro >= 1.3.2', 'avro >= 1.11.1']
LEGACYAVRO_REQUIRES = ['fastavro >= 1.3.2', 'avro-python3 >= 1.10.1']
JSON_REQUIRES = ['jsonschema >= 3.2.0']
PROTO_REQUIRES = ['protobuf >= 4.22.0']
ALL = AVRO_REQUIRES + LEGACYAVRO_REQUIRES + JSON_REQUIRES + PROTO_REQUIRES
setup(name         = 'robotframework-confluentkafkalibrary',
      version      = VERSION,
      description  = 'Confluent Kafka library for Robot Framework',
      long_description = DESCRIPTION,
      author       = 'Robert Karasek',
      author_email = '<robo.karasek@gmail.com>',
      url          = 'https://github.com/robooo/robotframework-ConfluentKafkaLibrary',
      license      = 'Apache License 2.0',
      keywords     = 'robotframework confluent kafka',
      platforms    = 'any',
      classifiers  = [
          "License :: OSI Approved :: Apache Software License",
          "Operating System :: OS Independent",
          "Programming Language :: Python",
          "Topic :: Software Development :: Testing"
      ],
      install_requires = [
          'robotframework >= 3.2.1',
          'confluent-kafka == 2.4.0',
          'requests >= 2.25.1',
      ],
      extras_require={
          'all': ALL,
          'avro': AVRO_REQUIRES,
          'legacyavro': LEGACYAVRO_REQUIRES,
          'json': JSON_REQUIRES,
          'protobuf': PROTO_REQUIRES,
      },
      package_dir  = {'' : 'src'},
      packages     = ['ConfluentKafkaLibrary'],
      )
