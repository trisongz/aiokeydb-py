import os
import sys
from pathlib import Path
from setuptools import setup, find_packages

if sys.version_info.major != 3:
    raise RuntimeError("This package requires Python 3+")

version = '0.0.3'
pkg_name = 'aiokeydb'
gitrepo = 'trisongz/aiokeydb-py'
root = Path(__file__).parent

requirements = [
    'aioredis',
    'pydantic'
]

extras = {
    'operator': ['watchfiles']
}

args = {
    'packages': find_packages(include = ['aiokeydb', 'aiokeydb.*']),
    'install_requires': requirements,
    'include_package_data': True,
    'long_description': root.joinpath('README.md').read_text(encoding='utf-8'),
    'entry_points': {}
}

if extras: args['extras_require'] = extras

setup(
    name=pkg_name,
    version=version,
    url=f'https://github.com/{gitrepo}',
    license='MIT Style',
    description='asyncio KeyDB support',
    author='Tri Songz',
    author_email='ts@growthengineai.com',
    long_description_content_type="text/markdown",
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.7',
        'Topic :: Software Development :: Libraries',
    ],
    **args
)