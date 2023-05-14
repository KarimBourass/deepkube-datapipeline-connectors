from setuptools import setup, find_packages

requires = [
    "azure-mgmt-datafactory==0.14.0",
    "azure-storage-blob==12.4.0",
    "azure-storage-file-share==12.1.2",
    "pymssql",
    "pyodbc",
    "psycopg2",
    "pandas",
    "sqlalchemy",
    "pyarrow",
    "cx_oracle",
    "pymongo",
    "boto3"
]
setup(
    name='connectors',
    version='0.1.0',
    description='A set of connectors to import and upload data from/to multiple storage',
    url='git@github.com:KarimBourass/deepkube-datapipeline-connectors.git',
    author='Karim Bourass',
    license='unlicense',
    packages=find_packages(),
    install_requires=requires,
)
