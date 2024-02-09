import os
from urllib.request import urlretrieve
from zipfile import ZipFile
from scripts.constants import *

class DataHandler:
    def __init__(self):
        # URLS/URL TEMPLATES
        self.TAXI_URL_TEMPLATE = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
        self.NYPD_ARRESTS = "https://data.cityofnewyork.us/api/views/8h9b-rp9u/rows.csv?accessType=DOWNLOAD"
        self.NYPD_COURT_SUMMONS = "https://data.cityofnewyork.us/api/views/sv2w-rv3k/rows.csv?accessType=DOWNLOAD"
        self.NYPD_SHOOTING = "https://data.cityofnewyork.us/api/views/833y-fsy8/rows.csv?accessType=DOWNLOAD"
        self.NYPD_COMPLAINTS = "https://data.cityofnewyork.us/api/views/qgea-i56i/rows.csv?accessType=DOWNLOAD"
        self.NYPD_PRECICNTS_URL = "https://data.cityofnewyork.us/api/geospatial/78dh-3ptz?method=export&format=Original"
        self.NY_ZONE_SHP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"
        self.NY_ZONE_LOOKUP_URL= "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
        return

    def download_data(self):
        """
        Downloads data to be used for the analysis
        """
        # Make the directories
        self.make_landing_directories()
        return
    
    def make_landing_directories(self):
        """
        Generate directores to store landing data
        """
        # Taxi Data Folders
        taxi_paths = [LANDING_GREEN_DIR, LANDING_FHV_DIR, LANDING_YELLOW_DIR, LANDING_HVFHV_DIR]
        if not os.path.exists(taxi_paths):
            os.makedirs(taxi_paths)
        
        # Taxi Zone Data
        if not os.path.exists(ZONE_DIR):
            os.makedirs(ZONE_DIR)
        urlretrieve(self.NY_ZONE_SHP_URL, NY_ZONE_SHP_FILE)
        with ZipFile(NY_ZONE_SHP_FILE) as fp:
            fp.extractall(ZONE_DIR)
        urlretrieve(self.NY_ZONE_LOOKUP_URL, NY_ZONE_LOOKUP_FILE)
        
        # NYPD Shooting Data
        if not os.path.exists(LANDING_NYPD_SHOOTINGS):
            urlretrieve(self.NYPD_SHOOTING, LANDING_NYPD_SHOOTINGS)
        return
    
    def download_taxi_data(self):
        spark = create_spark()
        return