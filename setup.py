from setuptools import setup, find_packages

setup(
    name='zerog',
    version=open('VERSION').read().strip(),
    author='MotiveMetrics',
    install_requires=[
        'beanstalkc3>=0.4.0',
        'couchbase>=2.5.10',
        'marshmallow>=3.0.5',
        'tornado>=4.5'
    ],
    packages=find_packages(exclude=["tests"])
)
