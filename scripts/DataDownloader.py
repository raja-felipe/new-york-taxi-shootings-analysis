import os
from urllib.request import urlopen, urlretrieve
from zipfile import ZipFile
from constants import *

class DataDownloader:
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

    def to_landing(self):
        """
        Downloads data to be used for the analysis
        """
        # Make the directories
        self.make_landing_directories()
        self.download_taxi_data()
        self.download_nypd_data()
        self.download_taxi_zone_data()
        return
    
    def make_landing_directories(self):
        """
        Generate directores to store landing data
        """
        # Taxi Data Folders
        taxi_paths = LANDING_TAXI_DIRECTORIES
        for path in taxi_paths:
            if not os.path.exists(path):
                os.makedirs(path)
        if not os.path.exists(LANDING_NYPD_DIR):
            os.makedirs(LANDING_NYPD_DIR)
        if not os.path.exists(ZONE_DIR):
            print("MADE ZONE DIR")
            os.makedirs(ZONE_DIR)
        return
    
    def download_taxi_data(self) -> None:
        for year in YEARS:
            print(f"Begin year {year}")
            for month in MONTHS:
                # 0-fill i.e 1 -> 01, 2 -> 02, etc
                month = str(month).zfill(2) 
                print(f"Begin month {month}")
                for i in range(len(VEHICLES)):
                    curr_vehicle = VEHICLES[i]
                    curr_directory = LANDING_TAXI_DIRECTORIES[i]
                    try:
                        print(f'{month}-{year}-{curr_vehicle}')
                        # generate url
                        url = f'{TAXI_URL_TEMPLATE}{curr_vehicle}_tripdata_{year}-{month}{PARQUET}'
                        # generate output location and filename
                        if not os.path.exists(f'{curr_directory}/{year}/'):
                            os.makedirs(f'{curr_directory}/{year}/')
                        output_dir = f"{curr_directory}/{year}/{curr_vehicle}_tripdata_{year}-{month}{PARQUET}"
                        if not os.path.exists(output_dir):
                            # download
                            # resp = urlopen(url)
                            # with open(output_dir, "wb") as fp:
                            #     fp.write(resp.read())
                            urlretrieve(url, output_dir) 

                    except:
                        print(f'{month}-{year}-{curr_vehicle} FAILED')
                        pass

                print(f"Completed month {month}")
            print(f'Completed year {year}')
        return
    
    def download_nypd_data(self) -> None:
        # NYPD Shooting Data
        if not os.path.exists(LANDING_NYPD_SHOOTINGS):
            urlretrieve(self.NYPD_SHOOTING, LANDING_NYPD_SHOOTINGS)
        return
        
    
    def download_taxi_zone_data(self) -> None:
        # Taxi Zone Data
        urlretrieve(self.NY_ZONE_SHP_URL, NY_ZONE_SHP_FILE)
        with ZipFile(NY_ZONE_SHP_FILE) as fp:
            fp.extractall(ZONE_DIR)
        urlretrieve(self.NY_ZONE_LOOKUP_URL, NY_ZONE_LOOKUP_FILE)
        return

if __name__ == "__main__":
    data_downloader = DataDownloader()
    data_downloader.to_landing()