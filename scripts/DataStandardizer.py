from constants import *
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType, TimestampNTZType
from functools import reduce
import os

class DataStandardizer:
    def __init__(self):
        self.data_schema = StructType([
            StructField('vendorid',
                        LongType(), True),
            StructField('ratecodeid',
                        DoubleType(), True),
            StructField('dolocationid',
                        LongType(), True),
            StructField('pulocationid',
                        LongType(), True),
            StructField('pickup_datetime',
                        TimestampNTZType(), True),
            StructField('dropoff_datetime',
                        TimestampNTZType(), True),
            StructField('taxi_color',
                        StringType(), True),
            StructField('trip_distance',
                        DoubleType(), True),
            StructField('passenger_count',
                        IntegerType(), True),
            StructField('payment_type',
                        LongType(), True),
            StructField('total_amount',
                        DoubleType(), True),
            StructField('fare_amount',
                        DoubleType(), True),
            StructField('tolls_amount',
                        DoubleType(), True),
            StructField('tip_amount',
                        DoubleType(), True),
            StructField('extra',
                        DoubleType(), True),
            StructField('mta_tax',
                        DoubleType(), True),
            StructField('congestion_surcharge',
                        DoubleType(), True),
            StructField('improvement_surcharge',
                        DoubleType(), True),
            StructField('ehail_fee',
                        DoubleType(), True),
            StructField('trip_type',
                        DoubleType(), True),
        ])
        return
    
    def standardize_green_taxi_data(self) -> None:
        spark = create_spark()

        # First create the necessary package
        if not os.path.exists(RAW_DIR):
            os.makedirs(RAW_DIR)

        if not os.path.exists(RAW_GREEN_DIR):
             os.makedirs(RAW_GREEN_DIR)

        # Now look through each of the green taxi data types
        for year in YEARS:
            curr_year_folder = f"{RAW_GREEN_DIR}{year}/"
            if not os.path.exists(curr_year_folder):
                 os.makedirs(curr_year_folder)
            for month in MONTHS:
                    curr_dir = LANDING_TAXI_DIRECTORIES[GREEN_VAL]
                    curr_vehicle = VEHICLES[GREEN_VAL]
                    month = str(month).zfill(2)
                    curr_file = f'{curr_dir}/{year}/{curr_vehicle}_tripdata_{year}-{month}{PARQUET}'
                    curr_green = spark.read.parquet(curr_file)
                    
                    # Now rename some of the columns of current dataframe
                    curr_green = curr_green.withColumnRenamed(GREEN_PICKUP_DATETIME, PICKUP_DATETIME) \
                        .withColumnRenamed(GREEN_DROPOFF_DATETIME, DROPOFF_DATETIME)
                    
                    # # Add remaining columns from other types of taxis
                    # curr_green = curr_green.withColumn(AIRPORT_FEE, F.lit(AIRPORT_FEE_SET))
                    curr_green = curr_green.select(COLUMN_ORDER)

                    # Column casing
                    green_col_casing = [F.col(col_name).alias(col_name.lower()) \
                                        for col_name in curr_green.columns]
                    curr_green = curr_green.select(*green_col_casing)
                    
                    # Set data types
                    curr_green = curr_green \
                        .select([F.col(c).cast(self.data_schema[i].dataType) for i, c in enumerate(curr_green.columns)])
                    
                    # Individually extract specific pickup/dropoff times
                    curr_green = curr_green.withColumn(PICKUP_DATE, F.to_date(F.col(PICKUP_DATETIME))) \
                                            .withColumn(DROPOFF_DATE, F.to_date(F.col(DROPOFF_DATETIME))) \
                                            .withColumn(PICKUP_TIME, F.date_format(F.col(PICKUP_DATETIME), TIME_FORMAT)) \
                                            .withColumn(DROPOFF_TIME, F.date_format(F.col(DROPOFF_DATETIME), TIME_FORMAT)) \
                                            .withColumn(TRIP_TIME_SECONDS, F.unix_timestamp(F.col(DROPOFF_DATETIME)) \
                                                        - F.unix_timestamp(F.col(PICKUP_DATETIME)))
                    
                    # Now save the new dataframe to the raw directory
                    green_file_path = f'{curr_year_folder}{curr_vehicle}_tripdata_{year}-{month}{PARQUET}'
                    if not os.path.exists(green_file_path):
                        curr_green.write.parquet(green_file_path)
        return
    
    def standardize_nypd_data(self):
        # Make the spark session
        spark = create_spark()

        # First make the raw directory
        if not os.path.exists(RAW_NYPD_DIR):
             os.makedirs(RAW_NYPD_DIR)

        # Get the shootings data from the landing directory
        landing_shootings = spark.read.csv(LANDING_NYPD_SHOOTINGS)

        landing_shootings = landing_shootings.distinct()
        # Change column names
        old_cols = landing_shootings.columns
        new_cols = list(landing_shootings.head(1)[0])
        landing_shootings = reduce(lambda data, idx: data.withColumnRenamed(old_cols[idx], new_cols[idx]), 
                            range(len(old_cols)), landing_shootings)
        # Remove unnecessary row
        landing_shootings = landing_shootings.filter(F.col(NYPD_ROW_FILTERS[0]) != NYPD_ROW_FILTERS[0])
        landing_shootings = landing_shootings.withColumn(YEAR, F.col(NYPD_DATE_COLUMNS[0]).substr(7, 4))
        landing_shootings = landing_shootings.withColumn(MONTH, F.col(NYPD_DATE_COLUMNS[0]).substr(1, 2))
        landing_shootings = landing_shootings.withColumn(DAY, F.col(NYPD_DATE_COLUMNS[0]).substr(4, 2))
        # Only get from 2016 onwards
        landing_shootings = landing_shootings.filter(F.col(YEAR) >= FROM_YEAR)
        # Column casing
        col_casing = [F.col(col_name).alias(col_name.lower()) for col_name in landing_shootings.columns]
        landing_shootings = landing_shootings.select(*col_casing)
        # Save the file
        curr_name = RAW_SHOOTINGS_DIR
        if not os.path.exists(curr_name):
            landing_shootings.write.parquet(curr_name)

        return
    
if __name__ == "__main__":
    data_standardizer = DataStandardizer()
    data_standardizer.standardize_green_taxi_data()
    data_standardizer.standardize_nypd_data()