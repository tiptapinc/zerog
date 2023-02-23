from setuptools import setup, find_packages

setup(
    name='zerog',
    version=open('VERSION').read().strip(),
    author='MotiveMetrics',
    install_requires=[
        'beanstalkc3>=0.4.0',
        'couchbase>=3.2.7',
        'marshmallow>=3.0.5',
        'psutil',
        'tornado>=4.5'
    ],
    packages=find_packages(exclude=["tests"])
)
