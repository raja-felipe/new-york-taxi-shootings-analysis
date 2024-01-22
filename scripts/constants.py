# Spark Session Initialization
SPARK_APP_NAME = "MAST30034 Project 1"
SPARK_EAGER_VAL = "spark.sql.repl.eagerEval.enabled"
SPARK_EAGER_VAL_SET = True
SPARK_CACHE_METADATA = "spark.sql.parquet.cacheMetadata"
SPARK_CACHE_METADATA_SET = "true"
SPARK_TIMEZONE = "spark.sql.session.timeZone"
SPARK_TIMEZONE_SET = "Etc/UTC"
SPARK_DRIVER_MEMORY = "spark.driver.memory"
SPARK_DRIVER_MEMORY_SET = "100g"
SPARK_AUTOBROADCAST_THRESHOLD = "spark.sql.autoBroadcastJoinThreshold"
SPARK_AUTOBROADCAST_THRESHOLD_SET = "-1"
SPARK_EXECUTOR_MEM_OVERHEAD = 'spark.executor.memoryOverhead'
SPARK_EXECUTOR_MEM_OVERHEAD_SET = '1500'

# URLS/URL TEMPLATES
TAXI_URL_TEMPLATE = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
NYPD_ARRESTS = "https://data.cityofnewyork.us/api/views/8h9b-rp9u/rows.csv?accessType=DOWNLOAD"
NYPD_COURT_SUMMONS = "https://data.cityofnewyork.us/api/views/sv2w-rv3k/rows.csv?accessType=DOWNLOAD"
NYPD_SHOOTING = "https://data.cityofnewyork.us/api/views/833y-fsy8/rows.csv?accessType=DOWNLOAD"
NYPD_COMPLAINTS = "https://data.cityofnewyork.us/api/views/qgea-i56i/rows.csv?accessType=DOWNLOAD"
NYPD_PRECICNTS_URL = "https://data.cityofnewyork.us/api/geospatial/78dh-3ptz?method=export&format=Original"
NY_ZONE_SHP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"
NY_ZONE_LOOKUP_URL= "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"

# Keywords for File/Variable Names
FROM_YEAR = 2016
TO_YEAR = 2020
COVID_BOUND = 2021
DOWNLOAD_BOUND = 2023
FROM_MONTH = 1
TO_MONTH = 12
FROM_DAY = 1
TO_DAY = 31
YEARS = range(FROM_YEAR,TO_YEAR)
MONTHS = range(1,13)
MONTH_ZIP = 2

YELLOW = 'yellow'
YELLOW_VAL = 0
GREEN = "green"
GREEN_VAL = 1
FHV = "fhv"
FHV_VAL = 2
HVFHV = "hvfhv"
HVFHV_VAL = 3
TAXI = "taxi"
NYPD = "NYPD"
VEHICLES = [YELLOW, GREEN]

HOUR = "hour"
HOUR_MIN = 0
HOUR_MAX = 23

DATE = "date"
TIME = "time"
RAW_ANALYSIS_FILES = "raw_analysis_files"
YELLOW_FILE_FRMT = f'{YELLOW}_tripdata_'
GREEN_FILE_FRMT = f'{GREEN}_tripdata_'

SHOOTINGS = "shootings"
PARQUET = '.parquet'
NUM_MONTHS_BACK= -17
SUM = "sum"
FIRST = "first"
OVERWRITE = "overwrite"
NY_TZ = "America/New_York"
AIRPORT = "Airport"
ZONE = "Zone"
SERVICE_ZONE = "service_zone"
YELLOW_ZONE = "Yellow Zone"

# Designated Directories
LANDING_DIR = "./data/landing/"
RAW_DIR = "./data/raw/"
CURATED_DIR = "./data/curated/"
ZONE_DIR = f'.{LANDING_DIR}zone/'
TAXI_DIR = f'.{LANDING_DIR}{TAXI}/'
GREEN_DIR = f'.{TAXI_DIR}{GREEN}/'
YELLOW_DIR = f'.{TAXI_DIR}{YELLOW}/'
FHV_DIR = f'.{TAXI_DIR}{FHV}/'
HVFHV_DIR = f'.{TAXI_DIR}{HVFHV}/'