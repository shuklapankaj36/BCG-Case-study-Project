import yaml
import logging
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DataLoader") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
    .getOrCreate()

# Configure logger for utils
logger = logging.getLogger(__name__)

class ConfigLoader:

    """ his class is responsible for loading the configuration file. """

    def __init__(self, config_path):
        self.config_path = config_path

    def load_config(self):
        """
        Load the configuration file to get output path and other configurations.
        """
        try:
            with open(self.config_path, 'r') as file:
                config = yaml.safe_load(file)
            logger.info(f"Configuration loaded from {self.config_path}")
            return config
        except Exception as e:
            logger.error(f"Error loading configuration file: {e}")
            raise


class DataLoader:
    
    """  This class is responsible for loading the datasets."""
    def __init__(self, dataset_paths):
        self.dataset_paths = dataset_paths
        
    def load_data(self):
        """
        Load the datasets into pyspark DataFrames.
        """
        dataframes = {}
        for dataset_name, dataset_path in self.dataset_paths.items():
            try:
                logger.info(f"Loading dataset: {dataset_name} from {dataset_path}")
                dataframes[dataset_name] = spark.read.csv(dataset_path, header=True, inferSchema=True)
            except FileNotFoundError as e:
                logger.error(f"Dataset file not found: {dataset_path}")
                raise
            except Exception as e:
                logger.error(f"Error loading dataset {dataset_name} from {dataset_path}: {str(e)}")
                raise
        return dataframes
