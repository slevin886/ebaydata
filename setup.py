#!/usr/bin/env python

from distutils.core import setup, find_packages

setup(name='ebaydata',
      version='1.0',
      description='Pulls ebay data for given keywords and returns an easily analysed csv or dataframe with no embedded objects',
      author='Scott Levin',
      author_email='slevin886@gmail.com',
      keywords='ebay data analysis analyze shop shopping product info easy',
      url='https://github.com/slevin886/ebaydata',
      packages=[find_packages()],
      include_package_data=True,
      zip_safe=False,
      install_requires=['ebaysdk==2.1.5', 'pandas==0.24.2', 'tqdm==4.31.1']
      license='MIT'
      )
