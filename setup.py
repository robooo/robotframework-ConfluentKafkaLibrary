#!/usr/bin/env python

from os.path import join, dirname
from setuptools import setup

filename=join(dirname(__file__), 'src', 'ConfluentKafkaLibrary', 'version.py')
exec(compile(open(filename).read(),filename, 'exec'))

DESCRIPTION = """
Confluent Kafka wrapped in Robot Framework.
"""[1:-1]

AVRO_REQUIRES = ['fastavro >= 1.3.2', 'avro >= 1.11.1']
JSON_REQUIRES = ['jsonschema >= 3.2.0', 'pyrsistent >= 0.20.0']
PROTO_REQUIRES = ['protobuf >= 4.22.0', 'googleapis-common-protos >= 1.66.0']
SCHEMA_REGISTRY_REQUIRES = ['httpx>=0.26', 'cachetools >= 5.5.0', 'attrs >= 24.3.0']
ALL = AVRO_REQUIRES + JSON_REQUIRES + PROTO_REQUIRES + SCHEMA_REGISTRY_REQUIRES
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
          'confluent-kafka == 2.8.0',
          'requests >= 2.25.1',
      ],
      extras_require={
          'all': ALL,
          'avro': AVRO_REQUIRES,
          'json': JSON_REQUIRES,
          'protobuf': PROTO_REQUIRES,
          'schemaregistry': SCHEMA_REGISTRY_REQUIRES,
      },
      package_dir  = {'' : 'src'},
      packages     = ['ConfluentKafkaLibrary'],
      )
