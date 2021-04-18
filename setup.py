#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright © 2021, Matjaž Guštin <dev@matjaz.it> <https://matjaz.it>.
# Released under the BSD 3-Clause License

"""Package setup for the SQLiteCls library."""

from distutils.core import setup

# noinspection PyUnresolvedReferences
import setuptools

setup(
    name='SQLiteCls',
    version='0.1.0',
    description='',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author='Matjaž Guštin',
    author_email='dev@matjaz.it',
    url='https://github.com/TheMatjaz/SQLiteCls',
    license='BSD',
    py_modules=[
        'sqlitecls',
    ],
    keywords=[
        'sqlite',
        'sqlite3',
        'database',
        'wrapper',
        'class',
        'with',
    ],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Topic :: Software Development',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
    python_requires='>=3',
)
