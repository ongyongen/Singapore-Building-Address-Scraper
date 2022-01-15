# Required libraries
import pandas as pd
import numpy as np
import geopandas as gpd
import shapely
from shapely.geometry import Point

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from json import JSONDecodeError

import multiprocessing
import concurrent.futures

# Parameters for Scraper.py
postal_code_range = 830000
max_workers = 2 * multiprocessing.cpu_count() + 1
name_of_scraped_address_file = 'sg_address.csv'

# Parameters for cleanFile.py
df = pd.read_csv('sg_address.csv')
pa = gpd.read_file('map.geojson')

name_of_saved_file = "sg_addresses_cleaned.csv"