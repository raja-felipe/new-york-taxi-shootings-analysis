from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType
from functools import reduce
from pyspark.sql import functions as F
import os
from constants import *
import pandas as pd
import geopandas as gpd
import numpy as np
import pyproj


class DataCurator:
    def __init__(self):
        return
    
    def make_valid_taxi_zones(self) -> tuple:
        """
        Gets taxi zones associated with high numbers of shootings
        - Return
            -   Tuple containing lists of green-taxi valid:
                -   High shooting pick-up location ids
                -   High shooting drop-off location ids
                -   Non-high shooting pick-up location ids
                -   Non-high shooting drop-off location ids
                -   While ensuring these locations are accessible by green taxis
        """
        # This Extracts all the good pickup and dropof zones
        # Also applies rules on YELLOW ZONES and AIRPORTS
        taxi_zones = pd.read_csv(TAXI_ZONE_LOOKUP_FILE)
        taxi_zones = taxi_zones.dropna()
        taxi_zones_list = list(taxi_zones[LOCATION_ID])
        airports = list(taxi_zones[taxi_zones[ZONE].\
                                str.contains(AIRPORT)][LOCATION_ID])
        yellow_zones = list(taxi_zones[taxi_zones[SERVICE_ZONE] \
                                    == YELLOW_ZONE][LOCATION_ID])
        banned_pick_ups = list(set(yellow_zones+airports))

        # Now filter based on the shooting zone
        top_shooting_dropoffs = [35, 74, 42, 177, 168, 61, 225, 78, 159, 169]

        top_shooting_pickups = [loc for loc in top_shooting_dropoffs \
                                if loc not in banned_pick_ups]
        remove_pickups = [col for col in taxi_zones_list \
                        if col not in top_shooting_pickups]
        remove_dropoffs = [col for col in taxi_zones_list \
                        if col not in top_shooting_dropoffs]

        return top_shooting_pickups, top_shooting_dropoffs, \
            remove_pickups, remove_dropoffs
    
    def curate_green_data(self) -> None:
        # Start a spark session
        spark =(
            SparkSession.builder.appName(SPARK_APP_NAME)
            .config(SPARK_EAGER_VAL, SPARK_EAGER_VAL_SET) 
            .config(SPARK_CACHE_METADATA, SPARK_CACHE_METADATA_SET)
            .config(SPARK_TIMEZONE, SPARK_TIMEZONE_SET)
            .config(SPARK_DRIVER_MEMORY, SPARK_DRIVER_MEMORY_SET)
            .config(SPARK_AUTOBROADCAST_THRESHOLD, \
                    SPARK_AUTOBROADCAST_THRESHOLD_SET)
            .config(SPARK_EXECUTOR_MEM_OVERHEAD, \
                    SPARK_EXECUTOR_MEM_OVERHEAD_SET)
            .getOrCreate()
        )

        # Create new directory
        if not os.path.exists(CURATED_GREEN_DIR):
            os.makedirs(CURATED_GREEN_DIR)
        
        # Need to compile all files for each year
        green_files = []
        for year in YEARS:
            curr_year_files = sorted([f'{RAW_GREEN_DIR}/{year}/{file}' \
                            for file in os.listdir(f'{RAW_GREEN_DIR}/{year}/')])
            green_files += curr_year_files
        
        # Now select only the files relevant to our data
        green_files = sorted(green_files[NUM_MONTHS_BACK:])
        raw_green = spark.read.load(green_files)

        # These columns are dropped because
        # - CONGESTION SURCHARGE and EHAIL FEE: No info about 
        #   them in data dictionary
        # - TOTAL AMOUNT: Does not actually represent real
        #   total amount (excludes cash tip)
        raw_green = raw_green.drop(CONGESTION_SURCHARGE)\
            .drop(EHAIL_FEE).drop(TOTAL_AMOUNT).drop(STORE_AND_FWD_FLAG)

        # Now we can adjust the timezone to NYC, since it was
        # kept in UTC
        raw_green = raw_green.withColumn(PICKUP_DATETIME, \
                                        F.from_utc_timestamp(F.col(PICKUP_DATETIME),\
                                                            F.lit(NY_TZ)))
        raw_green = raw_green.withColumn(DROPOFF_DATETIME, \
                                        F.from_utc_timestamp(F.col(DROPOFF_DATETIME),\
                                                            F.lit(NY_TZ)))
        
        # Remove negative trip durations
        raw_green = raw_green.filter((F.unix_timestamp(F.col(DROPOFF_DATETIME)) - \
                                    F.unix_timestamp(F.col(PICKUP_DATETIME))) >= 0)

        # Ensure Proper Taxi Vndors
        raw_green = raw_green.filter((F.col(VENDOR_ID) == TAXI_VENDOR_1) |\
                                    (F.col(VENDOR_ID) == TAXI_VENDOR_2))

        # Ensure Proper Location IDs
        # NOT ALLOWED TO PICK UP FROM AIRPORT (1, 132, 138)
        # Also cannot be picked up from YELLOW ZONES
        raw_green = raw_green.filter(
            (F.col(PICKUP_LOCATION_ID) >= MIN_LOCATION_ID) & \
                (F.col(PICKUP_LOCATION_ID) <= MAX_LOCATION_ID) & \
            (F.col(DROPOFF_LOCATION_ID) >= MIN_LOCATION_ID) & \
                (F.col(DROPOFF_LOCATION_ID) <= MAX_LOCATION_ID) 
        )
        # We will just do an extra filter on the airports that can't be accessed
        # Open the CSV of taxi zones
        # we only really want to consider the taxi pickups/dropoffs to these ids
        _, _ , remove_pickups, remove_dropoffs = self.make_valid_taxi_zones()

        # FILTERING PICKUP AND DROPOFF LOCATION IDS
        for id in remove_pickups:
            raw_green = raw_green.filter(F.col(PICKUP_LOCATION_ID) != id)
        for id in remove_dropoffs:
            raw_green = raw_green.filter(F.col(DROPOFF_LOCATION_ID) != id)

        # Make sure the trip distance is reasnoabl (>= 0.6 miles)
        raw_green = raw_green.filter(F.col(TRIP_DISTANCE) >= MIN_TRIP_DISTANCE)

        # Make sure the trip had passengers that weren't negative
        raw_green = raw_green.filter(F.col(PASSENGER_COUNT) > MIN_PASSENGERS)

        # Make sure the stoer and foward flags are within dictionary
        # raw_green = raw_green.filter((F.col(STORE_AND_FWD_FLAG) == STORE_AND_FWD_FLAG_Y) | \
        #                             (F.col(STORE_AND_FWD_FLAG) == STORE_AND_FWD_FLAG_N))

        # Rate code is within valid values
        raw_green = raw_green.filter((F.col(RATE_CODE_ID) >= MIN_RATE_CODE_ID) & \
                                    (F.col(RATE_CODE_ID) <= MAX_RATE_CODE_ID))

        # Payment type is within valid values
        raw_green = raw_green.filter((F.col(PAYMENT_TYPE) >= PAYMENT_TYPE_MIN) & \
                                    (F.col(PAYMENT_TYPE) <= PAYMENT_TYPE_MAX))

        # Fare amount is non-negative
        raw_green = raw_green.filter(F.col(FARE_AMOUNT) >= MIN_AMOUNT)

        # Tolls Amount is non-negative
        raw_green = raw_green.filter(F.col(TOLLS_AMOUNT) >= MIN_AMOUNT)

        # Very strict rules on EXTRA data
        # - $1 Denotes Overnight, so trips from 8:00PM - 6:00AM
        # - $5 Denotes Rush Hour, so weekdays and btween 4:00PM - 8:00PM
        raw_green = raw_green.filter(
            ((F.col(EXTRA) == OVERNIGHT) & (F.hour(F.col(PICKUP_DATETIME)) >= 20) & \
                (F.hour(F.col(PICKUP_DATETIME)) <= 6))| \
            ((F.col(EXTRA) == RUSH_HOUR) & (F.hour(F.col(PICKUP_DATETIME)) >= 16) & \
                (F.hour(F.col(PICKUP_DATETIME)) <= 20) & \
                (F.dayofweek(F.col(PICKUP_DATETIME)) != 1)) & \
                (F.dayofweek(F.col(PICKUP_DATETIME)) != 7)
            )

        # Tip Amounts can only be FROM CREDIT CARDS, other payment types HAVE TO BE 0
        raw_green = raw_green.filter(
            (F.col(TIP_AMOUNT).isNotNull()) & \
            ((F.col(TIP_AMOUNT) > MIN_AMOUNT) & (F.col(PAYMENT_TYPE) \
                                                == CREDIT_CARD) | \
            (F.col(TIP_AMOUNT) == MIN_AMOUNT))
            )

        # MTA Tax can only be $0.50
        raw_green = raw_green.filter(
            (F.col(MTA_TAX) == MTA_TAX_VAL))

        # Trip Type has to be within valid values
        raw_green = raw_green.filter((F.col(TRIP_TYPE) == TRIP_TYPE_HAIL) | \
                                    (F.col(TRIP_TYPE) == TRIP_TYPE_DISPATCH))

        # For green taxis, improvement surcharge is only charged if it was hailed
        # Otherwise, SHOULD BE 0
        raw_green = raw_green.filter(
            ((F.col(IMPRORVEMENT_SURCHARGE) == IMPRORVEMENT_SURCHARGE_VAL) & \
            (F.col(TRIP_TYPE) == TRIP_TYPE_HAIL)) | \
            ((F.col(IMPRORVEMENT_SURCHARGE) == MIN_AMOUNT) & \
            (F.col(TRIP_TYPE) != TRIP_TYPE_HAIL))
        )

        # We can already drop trip type here
        raw_green = raw_green.drop(TRIP_TYPE)

        # Drop all NA values
        raw_green = raw_green.dropna()


        # MAKINIG AND DROPPING COLUMNS
        # NOTE: For time-based columns, we will opt to use the PICKUP time
        # as the frame of reference for when drvies happen
        raw_green = raw_green.withColumn(DAY_OF_WEEK, \
                                        F.dayofweek(F.col(PICKUP_DATETIME)))
        
        raw_green = raw_green.withColumn(HOUR, F.hour(PICKUP_DATETIME))

        raw_green = raw_green.withColumn(MONTH, F.month(PICKUP_DATETIME))

        raw_green = raw_green.withColumn(DAY, F.dayofmonth(PICKUP_DATETIME))

        raw_green = raw_green.withColumn(YEAR, F.year(PICKUP_DATETIME))

        raw_green = raw_green.withColumn(DATE, F.make_date(YEAR, MONTH, DAY))

        raw_green = raw_green.withColumn(TAXI_COUNT, F.lit(1))

        raw_green = raw_green.select([DATE, TAXI_COUNT, PU_LOCATION_ID, \
                                    DO_LOCATION_ID, DAY_OF_WEEK, HOUR])

        raw_green.write.mode(OVERWRITE).parquet\
            (f'{FILTERED_GREEN_NAME}')
        return
    
    def ohe_taxi(self) -> None:
        """
        One hot encodes relevant categorical columns
        of the filtered taxi data
        """
        # Start a spark session
        spark =(
            SparkSession.builder.appName(SPARK_APP_NAME)
            .config(SPARK_EAGER_VAL, SPARK_EAGER_VAL_SET) 
            .config(SPARK_CACHE_METADATA, SPARK_CACHE_METADATA_SET)
            .config(SPARK_TIMEZONE, SPARK_TIMEZONE_SET)
            .config(SPARK_DRIVER_MEMORY, SPARK_DRIVER_MEMORY_SET)
            .config(SPARK_AUTOBROADCAST_THRESHOLD, SPARK_AUTOBROADCAST_THRESHOLD_SET)
            .config(SPARK_EXECUTOR_MEM_OVERHEAD, SPARK_EXECUTOR_MEM_OVERHEAD_SET)
            .getOrCreate()
        )
        raw_green = spark.read.parquet(f'{FILTERED_GREEN_NAME}')
        
        # Now we one hot encode with provided value ranges
        day_of_week_vals = range(FROM_DAY_OF_WEEK, TO_DAY_OF_WEEK+1)
        pu_location_id_vals, do_location_id_vals, _, _ = self.make_valid_taxi_zones()
        hour_vals = range(HOUR_MIN, HOUR_MAX+1)

        raw_green = reduce(
            lambda df, category: df.withColumn(f'{PU_LOCATION_ID}_{category}', \
                F.when(F.col(PU_LOCATION_ID) == category, F.lit(1)).otherwise(0)),
            pu_location_id_vals,
            raw_green
        )

        raw_green = reduce(
            lambda df, category: df.withColumn(f'{DO_LOCATION_ID}_{category}', \
                F.when(F.col(DO_LOCATION_ID) == category, F.lit(1)).otherwise(0)),
            do_location_id_vals,
            raw_green
        )

        raw_green = reduce(
            lambda df, category: df.withColumn(f'{DAY_OF_WEEK}_{category}', \
                F.when(F.col(DAY_OF_WEEK) == category, F.lit(1)).otherwise(0)),
            day_of_week_vals,
            raw_green
        )

        raw_green = reduce(
            lambda df, category: df.withColumn(f'{HOUR}_{category}', \
                F.when(F.col(HOUR) == category, F.lit(1)).otherwise(0)),
            hour_vals,
            raw_green
        )

        # Now drop all the unneeded columns
        raw_green = raw_green.drop(PU_LOCATION_ID).drop(DO_LOCATION_ID) \
            .drop(DAY_OF_WEEK).drop(HOUR)

        # Now we can save the curated data
        raw_green.write.mode(OVERWRITE).parquet\
            (f'{OHE_GREEN_NAME}')
        return
    
    def curate_shootings_data(self) -> None:
        """
        Filters the shooting data for various missing
        values present in the dataset
        """
        # Start a spark session
        spark =(
            SparkSession.builder.appName(SPARK_APP_NAME)
            .config(SPARK_EAGER_VAL, SPARK_EAGER_VAL_SET) 
            .config(SPARK_CACHE_METADATA, SPARK_CACHE_METADATA_SET)
            .config(SPARK_TIMEZONE, SPARK_TIMEZONE_SET)
            .config(SPARK_DRIVER_MEMORY, SPARK_DRIVER_MEMORY_SET)
            .config(SPARK_AUTOBROADCAST_THRESHOLD, SPARK_AUTOBROADCAST_THRESHOLD_SET)
            .config(SPARK_EXECUTOR_MEM_OVERHEAD, SPARK_EXECUTOR_MEM_OVERHEAD_SET)
            .getOrCreate()
        )

        # Make the curated shootings dir
        if not os.path.exists(CURATED_SHOOTINGS_DIR):
            os.makedirs(CURATED_SHOOTINGS_DIR)

        raw_shootings = spark.read.parquet(f'{RAW_SHOOTINGS_DIR}')

        # First we have to filter our year to the desired range
        SHOOTINGS_DATE_FORMAT = "MM/dd/yyyy"
        raw_shootings = raw_shootings.withColumn(SHOOTINGS_DATE.lower(), \
                                                F.to_date(F.col(SHOOTINGS_DATE.lower()), \
                                                        SHOOTINGS_DATE_FORMAT))
        raw_shootings = raw_shootings.withColumn(YEAR, F.year(F.col(SHOOTINGS_DATE.lower())))
        raw_shootings = raw_shootings.filter((F.col(YEAR) >= 2022) & \
                                            (F.col(YEAR) <= 2023))

        # Drop these columns, too many missing entries
        raw_shootings = raw_shootings.drop(LOC_DESC, 
                                            LOC_CLASSIFICATION_DESC, 
                                            LOC_OF_OCCUR_DESC, 
                                            PERP_AGE_GROUP, 
                                            PERP_RACE, PERP_SEX)
        
        # Now filter out all the NULL values present in the dataset
        # These strings are substituted for NULL
        NULL_STRING = "(null)"
        UNKNOWN = "UNKNOWN"
        for c, t in raw_shootings.dtypes:
            if t == "string":
                raw_shootings = raw_shootings.filter((F.col(c).isNotNull()) &\
                                                    (F.col(c) != NULL_STRING)\
                                                        & (F.col(c) != UNKNOWN))
            elif t not in ("string", "timestamp", "date", "timestamp_ntz"):
                raw_shootings = raw_shootings.filter(~(F.isnan(F.col(c))) &\
                                                    (F.col(c).isNotNull()))
            elif t in ("timestamp", "date", "timestamp_ntz"):
                raw_shootings = raw_shootings.filter(F.col(c).isNotNull())
        
        # We now want to add some new columns
        HOUR_IND = 0
        HOUR_LEN = 2

        raw_shootings = raw_shootings.withColumn(SHOOTINGS_DATE.lower(), \
                                                F.to_date(F.col(SHOOTINGS_DATE.lower()), \
                                                        SHOOTINGS_DATE_FORMAT))
        raw_shootings = raw_shootings.withColumn(HOUR, F.substring(F.col(OCCUR_TIME), \
                                                                HOUR_IND, \
                                                                    HOUR_LEN).cast("int"))
        raw_shootings = raw_shootings.withColumn(DAY_OF_WEEK, F.dayofweek\
                                                (F.col(SHOOTINGS_DATE.lower())))
        raw_shootings = raw_shootings.withColumn(SHOOTINGS_COUNT, F.lit(1))

        # Now that we have filtered all our values, we need to extract the specific
        # First get the relevant shape files
        taxi_sf = gpd.read_file(TAXI_SHAPE_FILE)
        taxi_sf = taxi_sf.to_crs(epsg=4326)

        # We have to extract longitude and latitude centroids
        # Calculate centroids of polygons and create a DataFrame with centroids' coordinates
        taxi_centroids = taxi_sf.centroid
        taxi_centroids_df = pd.DataFrame({SF_LATITUDE: taxi_centroids.y, \
                                        SF_LONGITUDE: taxi_centroids.x})

        taxi_sf = pd.merge(taxi_sf, taxi_centroids_df, left_index=True,\
                        right_index=True)


        zones = pd.read_csv(NY_ZONE_LOOKUP_FILE)
        
        # Now we get the taxi gdf
        taxi_gdf = gpd.GeoDataFrame(
            pd.merge(zones, taxi_sf, on=LOCATION_ID, how=INNER)
        )

        # We have to convert the x and y coordinates in the shootings data
        # with longitude and latitude values

        # Define the coordinate transformation using pyproj
        nad83_to_wgs84 = pyproj.Transformer.from_crs(pyproj.CRS("epsg:2263"), \
                                                    pyproj.CRS("epsg:4326"), \
                                                        always_xy=True).transform


        # Define a UDF to perform the transformation and return a STRUCT
        def transform_coordinates(x, y):
            lon, lat = nad83_to_wgs84(x, y)
            return (lon, lat)

        schema = StructType([
            StructField(LONGITUDE, DoubleType(), False),
            StructField(LATITUDE, DoubleType(), False)
        ])

        COORDINATES = "coordinates"

        transform_udf = F.udf(transform_coordinates, schema)

        # Add new columns for longitude and latitude using the UDF
        shootings_with_coords = raw_shootings.withColumn(COORDINATES, \
                                                        transform_udf(X_COORD_CD, Y_COORD_CD))

        # Extract longitude and latitude from the STRUCT
        shootings_with_coords = shootings_with_coords.withColumn\
            (LONGITUDE, shootings_with_coords[COORDINATES].longitude)\
                .withColumn(LATITUDE, shootings_with_coords[COORDINATES].latitude).drop(LON_LAT)


        # Now we will use the precinct shape file to 
        # We will use Euclidean distance in this case
        def calculate_distance(lon, lat) -> int:
            """
            Calculates euclidean distance of a pandas df row entry
            with each possible location's centroid in a Geos Dataframe
            - Parameters
                - Lon: Longitude input
                - Lat: Latitude input
            - Returns
                - Double representing distance
            """
            distances = np.sqrt((lon - taxi_gdf[SF_LONGITUDE])**2 +\
                                (lat - taxi_gdf[SF_LATITUDE])**2)
            min_distance_index = np.argmin(distances)
            return taxi_gdf[LOCATION_ID][min_distance_index]

        # Find nearest location by looping
        location_ids = []
        for _, row in shootings_with_coords.toPandas().iterrows():
            location_ids.append(calculate_distance(row[LONGITUDE], \
                                                row[LATITUDE]))

        pandas_shootings = shootings_with_coords.toPandas()
        locations_series = pd.Series(location_ids)

        pandas_shootings[TAXI_LOCATION_ID] = locations_series.astype(int)

        # Now that you have the locations, need to check if these
        # are related to the locations within top 10 shooting location ids
        # Base on dropoffs since that is the whole set of locations
        _, do_location_id_vals, _, _ = self.make_valid_taxi_zones()
        pandas_shootings = pandas_shootings[pandas_shootings[TAXI_LOCATION_ID]\
                                            .isin(do_location_id_vals)]
        
        # Now let's drop our unnecessary columns
        DROP_COLS = [OCCUR_TIME, BORO, PRECINCT,
                        X_COORD_CD, Y_COORD_CD, LATITUDE, 
                        LONGITUDE, YEAR, COORDINATES, 
                        INCIDENT_KEY]
        pandas_shootings = pandas_shootings.drop(DROP_COLS, axis=1)

        # Now we can save the dataframe
        pandas_shootings.to_csv(FILTERED_SHOOTINGS_NAME)
        return
    
    def ohe_shootings(self) -> None:
        """
        One hot encodes the relevant categorical variables
        of the shooting data
        """
        # Reopen the pandas shootings data
        pandas_shootings = pd.read_csv(FILTERED_SHOOTINGS_NAME, index_col=0)
        # Now we want to get the dummy variables of every categorical variable
        CATEGORICAL_VARS = [JURISDICTION_CODE,
                        STATISTICAL_MURDER_FLAG, VIC_AGE_GROUP,
                        VIC_SEX, VIC_RACE, HOUR, DAY_OF_WEEK, TAXI_LOCATION_ID]

        for col in CATEGORICAL_VARS:
            curr_dummies = pd.get_dummies(pandas_shootings[col])
            # Make sure for each value in the dummies, we rename properly
            rename_dict = {}
            for name in curr_dummies.columns:
                rename_dict[name] = f'{SHOOTINGS}_{col}_{name}'
            pandas_shootings = pd.concat([pandas_shootings, curr_dummies], axis=1)
            pandas_shootings = pandas_shootings.rename(columns=rename_dict)

        # Let's just apply column casing before we save the file,
        # a lot of values in the dataframe are UPPER CASE
        pandas_shootings.columns = pandas_shootings.columns.str.lower()
        pandas_shootings = pandas_shootings.drop(CATEGORICAL_VARS, axis=1)
        pandas_shootings.to_csv(OHE_SHOOTINGS_NAME)
        return
    
if __name__ == "__main__":
    data_curator = DataCurator()
    data_curator.curate_green_data()
    data_curator.curate_shootings_data()
    data_curator.ohe_shootings()
    data_curator.ohe_taxi()