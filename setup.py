from setuptools import find_packages, setup

setup(
    name='diabetes_code',
    packages=find_packages(),
    version='0.1.0',
    description='To create the RAP Diabetes pipeline for National Diabetes Audit programme',
    author='NHS_Digital',
    licence=['MIT','OGL']
    setup_requires=['pytest-runner','flake8'],
    tests_require=['pytest'],
)
