from pyspark.sql import SparkSession

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

def create_spark():
    return (
        SparkSession.builder.appName(SPARK_APP_NAME)
        .config(SPARK_EAGER_VAL, SPARK_EAGER_VAL_SET) 
        .config(SPARK_CACHE_METADATA, SPARK_CACHE_METADATA_SET)
        .config(SPARK_TIMEZONE, SPARK_TIMEZONE_SET)
        .getOrCreate()
    )

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
LANDING_GREEN_DIR = f'{LANDING_DIR}{GREEN}'
LANDING_YELLOW_DIR = f'{LANDING_DIR}{YELLOW}'
LANDING_FHV_DIR = f'{LANDING_DIR}{FHV}'
LANDING_HVFHV_DIR = f'{LANDING_DIR}{HVFHV}'
LANDING_NYPD_DIR = f"{LANDING_DIR}{NYPD}/"
LANDING_NYPD_SHOOTINGS = f'{LANDING_NYPD_DIR}shootings.csv'
ZONE_DIR = f"{LANDING_DIR}zone/"
NY_ZONE_LOOKUP_FILE = f'{ZONE_DIR}ny_zones_lookup.csv'
NY_ZONE_SHP_FILE = f'{ZONE_DIR}ny_zones.zip'
RAW_DIR = "./data/raw/"
CURATED_DIR = "./data/curated/"
ZONE_DIR = f'.{LANDING_DIR}zone/'
TAXI_DIR = f'.{LANDING_DIR}{TAXI}/'
GREEN_DIR = f'.{TAXI_DIR}{GREEN}/'
YELLOW_DIR = f'.{TAXI_DIR}{YELLOW}/'
FHV_DIR = f'.{TAXI_DIR}{FHV}/'
HVFHV_DIR = f'.{TAXI_DIR}{HVFHV}/'