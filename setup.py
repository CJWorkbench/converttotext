#!/usr/bin/env python

from setuptools import setup

setup(
    name="converttotext",
    version="0.0.1",
    description="Convert columns to text",
    author="Adam Hooper",
    author_email="adam@adamhooper.com",
    url="https://github.com/CJWorkbench/converttotext",
    packages=[""],
    py_modules=["converttotext"],
    install_requires=["cjwmodule~=1.5.0"],
    tests_require=["pytest"],
)
