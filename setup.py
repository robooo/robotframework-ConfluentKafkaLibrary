#!/usr/bin/env python

from os.path import join, dirname
from setuptools import setup

filename=join(dirname(__file__), 'src', 'ConfluentKafkaLibrary', 'version.py')
exec(compile(open(filename).read(),filename, 'exec'))

DESCRIPTION = """
Confluent Kafka wrapped in Robot Framework.
"""[1:-1]

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
          "Development Status :: 4 - Beta",
          "License :: OSI Approved :: Apache Software License",
          "Operating System :: OS Independent",
          "Programming Language :: Python",
          "Topic :: Software Development :: Testing"
      ],
      install_requires = [
          'robotframework == 3.1.1',
          'confluent-kafka == 1.0.0',
          'uuid==1.30',
      ],
      package_dir  = {'' : 'src'},
      packages    = ['ConfluentKafkaLibrary'],
      )
