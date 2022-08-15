#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
from setuptools import setup

with open("jdlfactory/include/VERSION", "r") as f:
    version = f.read().strip()

setup(
    name          = 'jdlfactory',
    version       = version,
    license       = 'BSD 3-Clause License',
    description   = 'Description text',
    url           = 'https://github.com/tklijnsma/jdlfactory.git',
    author        = 'Thomas Klijnsma',
    author_email  = 'tklijnsm@gmail.com',
    packages      = ['jdlfactory'],
    package_data  = {'jdlfactory': ['include/*', 'server/*']},
    include_package_data = True,
    zip_safe      = False,
    scripts       = []
    )
