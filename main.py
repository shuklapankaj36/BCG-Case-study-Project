import sys
import logging
from pyspark.sql import SparkSession
from src.analytics import Analytics
from src.utils import ConfigLoader, DataLoader
from src.process import DataSaver
from src.result_saver import ResultsSaver


# Configure logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """
    Main function that loads the data, initializes the Analytics class, and calls methods.
    Saves results into CSV files.
    """
    try:
        spark = SparkSession.builder \
            .appName("DataLoader") \
            .config("spark.hadoop.fs.defaultFS", "file:///") \
            .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
            .getOrCreate()
            
        config_loader = ConfigLoader(config_path="./config/config.yaml")
        config = config_loader.load_config()
        logger.info(f"Configuration Loaded: {config}")

        # Extract dataset from config
        dataset_paths = config['input']['datasets']

        # Load the data using DataLoader
        data_loader = DataLoader(dataset_paths=dataset_paths)
        dataframes = data_loader.load_data()
        if not dataframes:
            logger.error("No datasets were loaded. Exiting.")
            sys.exit(1)

        analytics = Analytics(
            primary_person_df=dataframes.get('primary_person_df'),
            units_df=dataframes.get('units_df'),
            charges_df=dataframes.get('Charges_df'),
            endorse_df=dataframes.get('Endorse_df'),
            damages_df=dataframes.get('Damages_df'),
            restrict_df=dataframes.get('Restrict_df')
        )

        logger.info("\nPerforming ...Analysis:1 ")
        male_deaths_result = analytics.male_death_analytics()

        logger.info("\n Performing...Analysis:2")
        two_wheelers_result = analytics.two_wheelers_analytics()

        logger.info("\nPerforming ...Analysis:3")
        vehicle_make_result = analytics.vehicle_make_analytics()
        vehicle_make_result.show()

        logger.info("\nPerforming ...Analysis:4")
        hit_and_run_result = analytics.hit_and_run_analytics()

        
        logger.info("\nPerforming ...Analysis:5")
        state, count = analytics.non_female_accidents_analytics()
        if state and count: 
                logger.info(f"State: {state}, Accidents: {count}")

                state_non_female = {
                    "State": state,
                    "Accident_Count": count
                }

        logger.info("\nPerforming ...Analysis:6")
        result_top_3_to_5 = analytics.get_top_injury_vehicles()

        logger.info("\nPerforming ...Analysis:7 ")
        body_style_ethnic_group_result = analytics.body_style_ethnic_group_analytics()

        logger.info("\nPerforming ...Analysis:8")
        top_zip_codes_result = analytics.top_zip_codes_with_alcohol_crashes()

        logger.info(f"\nPerforming ...Analysis:9")
        distinct_crash_count = analytics.get_count_distinct_crash_ids_no_damage_property()

        logger.info("\nPerforming ...Analysis:10")
        top_vehicle_makes_result = analytics.top_vehicle_makes_for_speeding()


        """  Saving output file """
        config_path = config['input']['config_path']
        data_saver = DataSaver(config_path,config_loader)

        results_saver = ResultsSaver(data_saver)
        results_saver.save_results(male_deaths_result, body_style_ethnic_group_result, two_wheelers_result, 
                                vehicle_make_result, hit_and_run_result, distinct_crash_count, result_top_3_to_5, 
                                top_zip_codes_result, top_vehicle_makes_result, state_non_female)
        
    except Exception as e:
            logger.error(f"Error during main execution: {e}")
            sys.exit(1)




if __name__ == "__main__":
    main()
