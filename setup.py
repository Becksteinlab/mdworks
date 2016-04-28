#! /usr/bin/python
"""Setuptools-based setup script for mdworks.

For a basic installation just type the command::

  python setup.py install

"""

from setuptools import setup

setup(name='mdworks',
      version='0.1.0-dev',
      description='molecular dynamics with fireworks',
      author='David Dotson',
      author_email='dotsdl@gmail.com',
      packages=['mdworks'],
      license='BSD',
      install_requires=['fireworks']
      )