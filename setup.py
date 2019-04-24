from setuptools import setup

setup(
    name='tt_async_requests',
    description='TipTap asynchronous requests manager for Tornado.',
    long_description=(
        '%s\n\n%s' % (
            open('README.md').read(),
            open('CHANGELOG.md').read()
        )
    ),
    version=open('VERSION').read().strip(),
    author='TipTap',
    install_requires=[
        'tornado',
    ],
    package_dir={'tt_async_requests': 'src'},
    packages=['tt_async_requests']
)
