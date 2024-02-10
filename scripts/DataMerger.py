from pyspark.sql import functions as F
from  constants import *

class DataMerger:
    def __init__(self):
        return

    def merge_curated(self) -> None:
        """
        Merges the aggregated green taxi and shootings data
        into one dataframe, removing unnecessary rows and settling
        data type mismatches
        """
        # Start the spark session and make the dataframes
        spark = create_spark()

        agg_shootings = spark.read.csv(AGGREGATED_SHOOTINGS_NAME, header=True)
        agg_shootings = agg_shootings.withColumn(MONTH, F.col(MONTH).cast("int"))
        agg_shootings = agg_shootings.withColumn(DAY, F.col(DAY).cast("int"))
        agg_green = spark.read.parquet(FINAL_GREEN, header=True)

        # Now we will do an INNER JOIN, since we only want  cases
        # where the dates of both a taxi ride and shooting intersect
        
        final_df = agg_green.join(
            agg_shootings, 
            agg_green[DATE] == agg_shootings[SHOOTINGS_DATE.lower()], 
            INNER)
        
        print(final_df.count())

        # Ensure that the datatype is correct for the merged dataframe
        shootings_col = [col for col in final_df.columns if SHOOTINGS in col]
        for col in shootings_col:
            final_df = final_df.withColumn(col, F.col(col).cast("int"))
        
        # Drop the date dataframe of the shootings
        UNNECESSARY_COL = "_c0"
        final_df = final_df.drop(SHOOTINGS_DATE.lower(), UNNECESSARY_COL)

        # Now save the datafrarme
        final_df.write.mode(OVERWRITE).parquet(FINAL_DF)
        return


if __name__ == "__main__":
    data_merger = DataMerger()
    data_merger.merge_curated()