from setuptools import setup, find_packages

setup(
    name="squeal",
    version="0.1.0",
    description="Message queues backed by an RDB",
    # long_description=open("README.md").read(),
    author="Alex Dodge",
    author_email="alexdodge@gmail.com",
    # url=
    license=open("LICENSE").read(),
    packages=find_packages(exclude=("tests",)),
)
