from setuptools import setup, find_packages
 
setup(
    name='mdp_databricks',
    version='0.1',
    packages=find_packages(),
    description='A collection of common functions for MDP Databricks notebooks',
    author='Mark Bateman',
    author_email='mark.bateman@leedsbuildingsociety.co.uk',
    url='https://dev.azure.com/LeedsBuildingSociety/Data%20Platform%20Engineering/_git/databricks',
    install_requires=[
        'pytest==8.3.3',
        'numpy==2.0.2',
        'pyspark==3.5.3',
        'pandas==2.2.3'
    ],
)