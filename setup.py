#!/usr/bin/env python
from setuptools import setup, find_packages
from imp import load_source
from os import path
import io

__version__ = load_source('cirruslib.version', 'cirruslib/version.py').__version__

here = path.abspath(path.dirname(__file__))

# get the dependencies and installs
with io.open(path.join(here, 'requirements.txt'), encoding='utf-8') as f:
    all_reqs = f.read().split('\n')

install_requires = [x.strip() for x in all_reqs if 'git+' not in x]
dependency_links = [x.strip().replace('git+', '') for x in all_reqs if 'git+' not in x]

setup(
    name='cirrus-lib',
    author='Matthew Hanson (matthewhanson), Element 84',
    version=__version__,
    description='Cirrus Library',
    url='https://github.com/cirrus-geo/cirrus-lib.git',
    license='Apache-2.0',
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8'
    ],
    keywords='',
    packages=find_packages(exclude=['docs', 'test*']),
    include_package_data=True,
    install_requires=install_requires,
    dependency_links=dependency_links,
)
