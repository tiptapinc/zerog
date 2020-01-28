from setuptools import setup, find_packages

setup(
    name='zerog',
    version=open('VERSION').read().strip(),
    author='MotiveMetrics',
    install_requires=[
        'tornado>=4.5', 'marshmallow>=3.0.5'
    ],
    packages=find_packages(exclude=["tests"])
    # package_dir={"zerog": "zerog"}
)
