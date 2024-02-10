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
DAY_OF_WEEK = "day_of_week"
FROM_DAY_OF_WEEK = 1
TO_DAY_OF_WEEK = 7
TAXI_COUNT = "taxi_count"

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
TAXI_ZONE_LOOKUP_FILE = f'{ZONE_DIR}ny_zones_lookup.csv'
TAXI_SHAPE_FILE = f'{ZONE_DIR}taxi_zones.shp'
RAW_DIR = "./data/raw/"
CURATED_DIR = "./data/curated/"
ZONE_DIR = f'{LANDING_DIR}zone/'
TAXI_DIR = f'{LANDING_DIR}{TAXI}/'
GREEN_DIR = f'{TAXI_DIR}{GREEN}/'
YELLOW_DIR = f'{TAXI_DIR}{YELLOW}/'
FHV_DIR = f'{TAXI_DIR}{FHV}/'
HVFHV_DIR = f'{TAXI_DIR}{FHVHV}/'
RAW_GREEN_DIR = f'{RAW_DIR}{GREEN}/'
RAW_NYPD_DIR = f'{RAW_DIR}{NYPD}/'
RAW_SHOOTINGS_DIR = f'{RAW_NYPD_DIR}shootings.csv'
TAXI_ZONE_LOOKUP_FILE = f'{ZONE_DIR}ny_zones_lookup.csv'
CURATED_GREEN_DIR = f'{CURATED_DIR}{GREEN}/'
FILTERED_GREEN_NAME = f'{CURATED_GREEN_DIR}/filtered_green{PARQUET}'
OHE_GREEN_NAME = f'{CURATED_GREEN_DIR}/encoded_green{PARQUET}'
AGGREGATED_GREEN_NAME = f'{CURATED_GREEN_DIR}/aggregated_green{PARQUET}'
FINAL_GREEN = f'{CURATED_GREEN_DIR}/final_green{PARQUET}'
CURATED_NYPD_DIR = f'{CURATED_DIR}{NYPD}/'
CURATED_SHOOTINGS_DIR = f'{CURATED_NYPD_DIR}shootings.csv'
FILTERED_SHOOTINGS_NAME = f'{CURATED_SHOOTINGS_DIR}/filtered_shootings.csv'
OHE_SHOOTINGS_NAME = f'{CURATED_SHOOTINGS_DIR}/ohe_shootings.csv'
AGGREGATED_SHOOTINGS_NAME = f'{CURATED_SHOOTINGS_DIR}/aggregated_shootings.csv'

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
CONGESTION_SURCHARGE = 'congestion_surcharge'
TOTAL_AMOUNT = "total_amount"
VENDOR_ID = "vendorid"
TAXI_VENDOR_1 = 1
TAXI_VENDOR_2 = 2
STORE_AND_FWD_FLAG = "store_and_fwd_flag"
STORE_AND_FWD_FLAG_Y = "Y"
STORE_AND_FWD_FLAG_N = "N"
RATE_CODE_ID = "ratecodeid"
MIN_RATE_CODE_ID = 1
MAX_RATE_CODE_ID = 6
TOTAL_AMOUNT = "total_amount"
FARE_AMOUNT = 'fare_amount'
TOLLS_AMOUNT = 'tolls_amount'
TIP_AMOUNT = 'tip_amount'
MIN_AMOUNT = 0
NEGATIVE_AMOUNTS = "negative_amounts"
EXTRA = 'extra'
RUSH_HOUR = 0.5
OVERNIGHT = 1
MTA_TAX = 'mta_tax'
MTA_TAX_VAL = 0.5

CONGESTION_SURCHARGE = 'congestion_surcharge'

IMPRORVEMENT_SURCHARGE = 'improvement_surcharge'
IMPRORVEMENT_SURCHARGE_VAL = 0.3
TRIP_TYPE = "trip_type"
TRIP_TYPE_SET = -1
TRIP_TYPE_HAIL = 1
TRIP_TYPE_DISPATCH = 2

PAYMENT_TYPE = "payment_type"
PAYMENT_TYPE_MIN = 1
CREDIT_CARD = 1
PAYMENT_TYPE_MAX = 6

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

SHOOTINGS_KEY = "INCIDENT_KEY"
SHOOTINGS_DATE = "OCCUR_DATE"
NYPD_ROW_FILTERS = [SHOOTINGS_KEY]
NYPD_DATE_COLUMNS = [SHOOTINGS_DATE]

LOCATION_ID = "LocationID"
PICKUP_LOCATION_ID = "pulocationid"
DROPOFF_LOCATION_ID = "dolocationid"
PU_LOCATION_ID = "pulocationid"
DO_LOCATION_ID = "dolocationid"
MIN_LOCATION_ID = 1
MAX_LOCATION_ID = 263

SHOOTINGS_KEY = "INCIDENT_KEY"
SHOOTINGS_DATE = "OCCUR_DATE"

INCIDENT_KEY = 'incident_key'
OCCUR_DATE = 'occur_date'
OCCUR_TIME = 'occur_time'
BORO = 'boro'
LOC_OF_OCCUR_DESC = 'loc_of_occur_desc'
LOC_DESC = "location_desc"
PRECINCT = 'precinct'
JURISDICTION_CODE = 'jurisdiction_code'
LOC_CLASSIFICATION_DESC = 'loc_classfctn_desc'
STATISTICAL_MURDER_FLAG = 'statistical_murder_flag'
PERP_AGE_GROUP = "perp_age_group"
PERP_SEX = "perp_sex"
PERP_RACE = "perp_race"
VIC_AGE_GROUP = 'vic_age_group'
VIC_SEX = 'vic_sex'
VIC_RACE = 'vic_race'
X_COORD_CD = 'x_coord_cd'
Y_COORD_CD = 'y_coord_cd'
LATITUDE = 'latitude'
LONGITUDE = 'longitude'
LON_LAT = 'lon_lat'
SHOOTINGS_COUNT = "shootings_count"

TIME_FORMAT = 'HH:mm:ss'
SF_LONGITUDE = "Longitude"
SF_LATITUDE = "Latitude"
LONGITUDE = "longitude"
LATITUDE = "latitude"
TAXI_LOCATION_ID = "taxi_location_id"
TAXI_COUNT = "taxi_count"

INNER = "inner"

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