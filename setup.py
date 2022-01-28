from setuptools import find_packages, setup

setup(
    name='deltatools',
    packages=find_packages(include=['deltatools']),
    version='0.2.3',
    description='Automates source to target loads from data lake to Delta Lake tables.',
    author='Thomas Bailey (thomasjohnbailey@gmail.com)',
    license='MIT',
)