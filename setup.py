import sys
from pathlib import Path
from setuptools import setup, find_packages

if sys.version_info.major != 3:
    raise RuntimeError("This package requires Python 3+")

pkg_name = 'aiokeydb'
gitrepo = 'trisongz/aiokeydb-py'
root = Path(__file__).parent
version = root.joinpath('aiokeydb/version.py').read_text().split('VERSION = ', 1)[-1].strip().replace('-', '').replace("'", '')


requirements = [
    "deprecated>=1.2.3",
    "packaging>=20.4",
    'importlib-metadata >= 1.0; python_version < "3.8"',
    'typing-extensions; python_version<"3.8"',
    "async-timeout>=4.0.2",
    'lazyops>=0.2.4',
    "pydantic",
    "anyio",
]

args = {
    'packages': find_packages(include=[
            "aiokeydb",
            "aiokeydb.asyncio",
            "aiokeydb.commands",
            "aiokeydb.commands.bf",
            "aiokeydb.commands.json",
            "aiokeydb.commands.search",
            "aiokeydb.commands.timeseries",
            "aiokeydb.commands.graph",
        ]),
    'install_requires': requirements,
    'include_package_data': True,
    'long_description': root.joinpath('README.md').read_text(encoding='utf-8'),
    'entry_points': {},
    'extras_require': {
        "hiredis": ["hiredis>=1.0.0"],
        "ocsp": ["cryptography>=36.0.1", "pyopenssl==20.0.1", "requests>=2.26.0"],
    },
}


setup(
    name=pkg_name,
    version=version,
    url=f'https://github.com/{gitrepo}',
    license='MIT Style',
    description='Python client for KeyDB database and key-value store',
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