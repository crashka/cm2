# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

setup(
    name='cm2',
    version='0.1',
    packages=find_packages(include=['cm2']),
    url='',
    license_file='LICENSE.txt',
    author='crash',
    author_email='',
    description='Classical Music 2 - internet playlist scraping and analysis',
    python_requires='>=3.10',
    install_requires=['regex',
                      'pyyaml',
                      'peewee',
                      'requests',
                      'beautifulsoup4',
                      'lxml',
                      'unidecode',
                      'ckautils'],
    entry_points={
        'console_scripts': [
            'schema  = cm2.schema:main',
            'refdata  = cm2.refdata:main'
        ]
    }
)
