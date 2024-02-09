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
TO_YEAR = 2023
FROM_MONTH = 1
TO_MONTH = 12
FROM_DAY = 1
TO_DAY = 31
YEARS = range(FROM_YEAR,TO_YEAR)
MONTHS = range(1,13)
MONTH_ZIP = 2

YELLOW = 'yellow'
YELLOW_VAL = 1
GREEN = "green"
GREEN_VAL = 0
FHV = "fhv"
FHV_VAL = 2
FHVHV = "fhvhv"
FHVHV_VAL = 3
TAXI = "taxi"
NYPD = "NYPD"
# VEHICLES = [YELLOW, GREEN, FHV, FHVHV]
VEHICLES = [GREEN]

HOUR = "hour"
HOUR_MIN = 0
HOUR_MAX = 23

DATE = "date"
TIME = "time"
RAW_ANALYSIS_FILES = "raw_analysis_files"
YELLOW_FILE_FRMT = f'{YELLOW}_tripdata_'
GREEN_FILE_FRMT = f'{GREEN}_tripdata_'
FHV_FILE_FRMT = f'{FHV}_tripdata_'
FHVHV_FILE_FRMT = f'{FHVHV}_tripdata_'

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
TAXI_URL_TEMPLATE = "https://d37ci6vzurychx.cloudfront.net/trip-data/"


# Designated Directories
LANDING_DIR = "./data/landing/"
LANDING_GREEN_DIR = f'{LANDING_DIR}{GREEN}'
LANDING_YELLOW_DIR = f'{LANDING_DIR}{YELLOW}'
LANDING_FHV_DIR = f'{LANDING_DIR}{FHV}'
LANDING_HVFHV_DIR = f'{LANDING_DIR}{FHVHV}'
# LANDING_TAXI_DIRECTORIES = [LANDING_YELLOW_DIR, LANDING_GREEN_DIR, LANDING_FHV_DIR, LANDING_HVFHV_DIR]
LANDING_TAXI_DIRECTORIES = [LANDING_GREEN_DIR]
LANDING_NYPD_DIR = f"{LANDING_DIR}{NYPD}/"
LANDING_NYPD_SHOOTINGS = f'{LANDING_NYPD_DIR}shootings.csv'
ZONE_DIR = f"{LANDING_DIR}zone/"
NY_ZONE_LOOKUP_FILE = f'{ZONE_DIR}ny_zones_lookup.csv'
NY_ZONE_SHP_FILE = f'{ZONE_DIR}ny_zones.zip'
RAW_DIR = "./data/raw/"
CURATED_DIR = "./data/curated/"
ZONE_DIR = f'{LANDING_DIR}zone/'
TAXI_DIR = f'{LANDING_DIR}{TAXI}/'
GREEN_DIR = f'{TAXI_DIR}{GREEN}/'
YELLOW_DIR = f'{TAXI_DIR}{YELLOW}/'
FHV_DIR = f'{TAXI_DIR}{FHV}/'
HVFHV_DIR = f'{TAXI_DIR}{FHVHV}/'
RAW_GREEN_DIR = f'{RAW_DIR}{GREEN}/'

# Needed Columns and Bounds
GREEN_PICKUP_DATETIME = "lpep_pickup_datetime"
GREEN_DROPOFF_DATETIME = "lpep_dropoff_datetime"
YELLOW_PICKUP_DATETIME = "tpep_pickup_datetime"
YELLOW_DROPOFF_DATETIME = "tpep_dropoff_datetime"
PICKUP_DATETIME = "pickup_datetime"
DROPOFF_DATETIME = "dropoff_datetime"

YEAR = "year"
MONTH = "month"
DAY = "day"
PICKUP_YEAR = "pickup_year"
PICKUP_DAY = "pickup_day"
PICKUP_MONTH = "pickup_month"
PICKUP_TIME = "pickup_time"
PICKUP_HOUR = "pickup_hour"
PICKUP_MINUTES = "pickup_minutes"
PICKUP_SECONDS = "pickup_seconds"
PICKUP_DATE = "pickup_date"
DROPOFF_YEAR = "dropoff_year"
DROPOFF_DAY = "dropoff_day"
DROPOFF_MONTH = "dropoff_month"
DROPOFF_TIME = "dropoff_time"
DROPOFF_HOUR = "dropoff_hour"
DROPOFF_MINUTES = "dropoff_minutes"
DROPOFF_SECONDS = "dropoff_seconds"
DROPOFF_DATE = "dropoff_date"
TRIP_TIME_SECONDS = "trip_time_seconds"

PASSENGER_COUNT = "passenger_count"
TAXI_COLOR = "taxi_color"

TRIP_DISTANCE = "trip_distance"
MIN_TRIP_DISTANCE = 0

PASSENGER_COUNT = "passenger_count"
MIN_PASSENGERS = 0

AIRPORT_FEE = "airport_fee"
AIRPORT_FEE_SET = 0

EHAIL_FEE = "ehail_fee"
EHAIL_FEE_SET = 0

TRIP_TYPE = "trip_type"
TRIP_TYPE_SET = -1

TIME_FORMAT = 'HH:mm:ss'

# Column order to merge taxi datasets
COLUMN_ORDER = ["vendorid", 
 "ratecodeid", 
 "dolocationid", 
 "pulocationid", 
 "pickup_datetime", 
 "dropoff_datetime",
 "trip_distance", 
 "passenger_count", 
 "store_and_fwd_flag",
 "payment_type",
 "total_amount",
 "fare_amount",  
 "tolls_amount", 
 "tip_amount",
 "extra",
 "mta_tax",
 "congestion_surcharge",
 "improvement_surcharge",
 "ehail_fee",
 "trip_type"]