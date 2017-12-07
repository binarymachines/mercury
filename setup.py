#!/usr/bin/env python

# setuptools script for Mercury data management framework 

import codecs
import os
import re

from setuptools import setup, find_packages
from pip.req import parse_requirements
from pip.download import PipSession


NAME = 'mercury'
VERSION = '0.9.0'
PACKAGES = find_packages(where='src')
DEPENDENCIES=['snap-micro',
              'docopt',
              'arrow',
              'Flask',
              'itsdangerous',
              'Jinja2',
              'kafka',
              'MarkupSafe',
              'PyYAML',
              'SQLAlchemy',
              'SQLAlchemy-Utils',
              'mysql-connector-python',
              'Werkzeug',
              'requests',
              'boto3',
              'botocore',
              'raven',
              'redis']

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name='mercury',
    version=VERSION,
    author='Dexter Taylor',
    author_email='binarymachineshop@gmail.com',
    platforms=['any'],
    scripts=['scripts/blok',
             'scripts/kolo',
             'scripts/seesv',
             'scripts/xlcr'],
    packages=find_packages(),
    install_requires=DEPENDENCIES,
    test_suite='tests',
    description=('Mercury: a framework for fluid ETL and data management'),
    license='MIT',
    keywords='ETL data pipeline framework',
    url='http://github.com/binarymachines/mercury',
    long_description=read('README.txt'),
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Topic :: Software Development'
    ]
)
