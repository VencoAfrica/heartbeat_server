# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

__version__ = "0.0.1"

install_requires = ["redis==2.10.6"]

setup(
	name="heartbeat_server",
	version=__version__,
	description='Heartbeat Server',
	author='Manqala',
	author_email='dev@manqala.com',
	packages=find_packages(),
	include_package_data=True,
	install_requires=install_requires,
	python_requires="~=3.7",
)
