import os
from constants import *
import pandas as pd

class DataAggregator:
    def __init__(self):
        return
    
    def aggregate_taxi_data(self) -> None:
        """
        Aggregate taxi data by date for time series model
        """
        # Create spark session first
        spark = create_spark()

        # Columns to just sum over
        raw_green = spark.read.parquet\
            (OHE_GREEN_NAME)

        agg_cols = [col for col in raw_green.columns if col != DATE
                    and col not in [f'{DAY_OF_WEEK}_{i}' for i in range(FROM_DAY_OF_WEEK, TO_DAY_OF_WEEK+1)]]
        agg_dict = {}
        for col in agg_cols:
            agg_dict[col] = SUM
        for i in range(len(range(TO_DAY_OF_WEEK))):
            agg_dict[f'{DAY_OF_WEEK}_{i+1}'] = SUM

        # Order and group the dataset by the date
        raw_green = raw_green.groupBy(DATE).agg(agg_dict)
        raw_green = raw_green.orderBy(DATE)

        # Save the file
        if not os.path.exists(CURATED_GREEN_DIR):
            os.makedirs(CURATED_GREEN_DIR)
        raw_green.write.mode(OVERWRITE).parquet\
            (f'{AGGREGATED_GREEN_NAME}')
        return


    def aggregate_shootings_data(self):
        """
        Aggregate shootings data by date for time series model
        """
        pandas_shootings = pd.read_csv(OHE_SHOOTINGS_NAME, index_col=0)

        # First we want to exclude columns like day of week since we're not trying to add these together
        cols_exclude = [f'{DAY_OF_WEEK}_{i}' for i in \
                        range(FROM_DAY_OF_WEEK, TO_DAY_OF_WEEK+1)] \
                        + [SHOOTINGS_DATE.lower()]
        
        # Now we make the aggregation dict
        agg_dict = {col: SUM if col not in cols_exclude else FIRST \
                    for col in pandas_shootings.columns \
                        if col != SHOOTINGS_DATE.lower()}

        aggregated_shootings = pandas_shootings.groupby(SHOOTINGS_DATE.lower())\
            .agg(agg_dict).reset_index()

        # Now save the aggregate
        aggregated_shootings.to_csv(AGGREGATED_SHOOTINGS_NAME)
        return

if __name__ == "__main__":
    data_aggregator = DataAggregator()
    data_aggregator.aggregate_taxi_data()
    data_aggregator.aggregate_shootings_data()