from setuptools import setup, find_packages

setup(
    name='tqdb',
    version='0.1',
    packages=find_packages(),
    package_data={'tqdb': ['tqdb/*']},
)