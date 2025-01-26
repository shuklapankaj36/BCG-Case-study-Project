
import os
import json
import logging
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame as SparkDataFrame


spark = SparkSession.builder \
    .appName("DataLoader") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
    .getOrCreate()


logger = logging.getLogger(__name__)

class DataSaver:

    """This class provides methods to save the results of the analytics to JSON files."""

    def __init__(self, config_path, config_loader):
        self.config_path = config_path
        self.config_loader = config_loader
        self.config = self.config_loader.load_config()
        self.output_dir = self.config['output']['directory']


    def save_to_json(self, df, output_key):
        try:

            output_file = self.config['output'][output_key]

            if not os.path.exists(self.output_dir):
                os.makedirs(self.output_dir)

            output_path = os.path.join(self.output_dir, output_file)


            logger.info(f"Saving DataFrame to: {output_path}")

            if isinstance(df, pd.Series):
                logger.warning("Input is a pandas.Series. Converting to DataFrame.")
                df = df.to_frame(name="value")  # Convert Series to DataFrame

            if isinstance(df, SparkDataFrame):
                if df.count() == 0:
                    logger.warning(f"DataFrame is empty. Saving empty DataFrame to {output_path}")
            elif isinstance(df, pd.DataFrame):
                if df.empty:
                    logger.warning(f"DataFrame is empty. Saving empty DataFrame to {output_path}")
            elif isinstance(df, (list, dict)):
                if len(df) == 0:
                    logger.warning(f"Data is empty. Saving empty data to {output_path}")
            else:
                logger.error(f"Unsupported DataFrame type: {type(df)}. Cannot save to JSON.")
                return

            if isinstance(df, SparkDataFrame):
                pandas_df = df.toPandas()
                pandas_df.to_json(output_path, orient="records", lines=True)
                logger.info(f"Successfully saved {output_file} to {output_path} (converted from PySpark to Pandas)")

            elif isinstance(df, pd.DataFrame):
                df.to_json(output_path, orient="records", lines=True)
                logger.info(f"Successfully saved {output_file} to {output_path}")

            elif isinstance(df, (list, dict)):
                with open(output_path, 'w') as json_file:
                    json.dump(df, json_file, indent=4)
                logger.info(f"Successfully saved {output_file} to {output_path}")

        except Exception as e:
            logger.error(f"Error saving DataFrame to JSON: {e}")
            raise

