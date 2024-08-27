
from setuptools import setup, find_packages
from os import listdir

descr = "Generate insurance data for testing and demonstrations"
name = 'general_insurance_data_model'
url = 'https://github.com/pjdyson/general_insurance_data_model'
version='0.0.1' # Put this in __init__.py

setup(
    name=name,
    version=version,
    maintainer='Peter Dyson',
    maintainer_email='peter@dysonmail.com',
    download_url=f'{url}/archive/v{version}.tar.gz',
    license='GNU3.0',
    url=url,
    description=descr,
    install_requires=[
        "pandas>=0.23.0",
        "numpy>=1.12.0",
        "chainladder>=0.7.0",
   ],
)
