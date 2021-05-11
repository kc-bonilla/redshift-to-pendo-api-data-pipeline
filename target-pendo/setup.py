#!/usr/bin/env python3

from setuptools import setup
from setuptools import find_packages

setup(name='target-pendo',
      version='0.1.0',
      description='Singer.io target to consume streams and interact with Pendo API Agent',
      author='ShootProof',
      url='',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['target_pendo'],
      install_requires=[
          'jsonschema>=2.6.0',
          'singer-python>=5.0.4'
      ],
      packages=find_packages(),
      include_package_data=True,
      entry_points= {
              'console_scripts': [
                  'target-pendo=target_pendo.__init__:main'
                  
              ]
          }
)
