#!/usr/bin/env python
import os
import os.path

from setuptools import setup, find_packages


HERE = os.path.abspath(os.path.dirname(__file__))
VERSION = os.environ.get('CIRRUS_VERSION', '0.0.0')


with open(os.path.join(HERE, 'README.md'), encoding='utf-8') as f:
    readme = f.read()

with open(os.path.join(HERE, 'requirements.txt'), encoding='utf-8') as f:
    reqs = f.read().split('\n')

install_requires = [x.strip() for x in reqs if 'git+' not in x]
dependency_links = [x.strip().replace('git+', '') for x in reqs if 'git+' not in x]


setup(
    name='cirrus-lib',
    author='Matthew Hanson (matthewhanson), Element 84',
    version=VERSION,
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
    packages=find_packages('src'),
    package_dir={'': 'src'},
    include_package_data=True,
    install_requires=install_requires,
    dependency_links=dependency_links,
)
