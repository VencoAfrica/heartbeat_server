# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

__version__ = "0.0.1"

install_requires = ["aioredis==1.3.1", "iec62056-21"]

setup(
	name="heartbeat_server",
	version=__version__,
	description='Heartbeat Server',
	author='Venco',
	author_email='devs@venco.co',
	packages=find_packages(),
	include_package_data=True,
	install_requires=install_requires,
	python_requires="~=3.7",
)
