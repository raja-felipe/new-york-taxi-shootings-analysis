from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
from constants import *
from pyspark.ml.functions import vector_to_array
from DataCurator import DataCurator

class DataRenamer:
    def __init__(self):
        return
    
    def rename_aggregate_taxi(self) -> None:
        """
        Ensure proper naming of columns, particularly
        removal of sum() or count() in name of 
        one hot encoded dataframe
        """
        spark = create_spark()

        # First reopen the aggregated dataset
        raw_taxi = spark.read.parquet(f'{AGGREGATED_GREEN_NAME}')

        # Make the rename dictionary remove the sum() part of column names
        change_dict = {}
        for col in raw_taxi.columns:
            # Format of this column name is sum(col)
            # print(col)
            if SUM in col:
                start = col.index("(") + 1
                end = col.index(")")
                column_name = col[start:end]
                change_dict[col] = column_name
            else:
                change_dict[col] = col

        # Rename the dataframe
        raw_taxi = raw_taxi.select(*[F.col(old).alias(new) \
                                    for old, new in change_dict.items()])
    
                
        # We need these to reset the order of the dataframe columns
        day_of_week_vals = range(FROM_DAY_OF_WEEK, TO_DAY_OF_WEEK+1)
        data_curator = DataCurator()
        pu_location_id_vals, do_location_id_vals, _, _ = data_curator.make_valid_taxi_zones()
        hour_vals = range(HOUR_MIN, HOUR_MAX+1)

        # Format the names to the dataframe
        day_names = [f'{DAY_OF_WEEK}_{category}' for category in day_of_week_vals]

        pu_location_names = [f'{PICKUP_LOCATION_ID}_{category}' \
                            for category in pu_location_id_vals]
        
        do_location_names = [f'{DROPOFF_LOCATION_ID}_{category}' \
                            for category in do_location_id_vals]
        hour_names = [f'{HOUR}_{category}' for category in hour_vals]

        # Set the column name
        column_order = [DATE] + pu_location_names + do_location_names \
            + hour_names + day_names + [TAXI_COUNT]
        raw_taxi = raw_taxi.select(column_order)

        print(raw_taxi.count())
        
        # Now save the dataframe
        raw_taxi.write.mode(OVERWRITE).parquet(f'{FINAL_GREEN}')            
        return
    
if __name__ == "__main__":
    data_renamer = DataRenamer()
    data_renamer.rename_aggregate_taxi()
    
