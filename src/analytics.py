from pyspark.sql.functions import col, sum as spark_sum
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import rank
import pandas as pd
from pyspark.sql import SparkSession
from src.utils import ConfigLoader



class Analytics:

    def __init__(self, primary_person_df, units_df, charges_df, endorse_df, damages_df, restrict_df):
        self.primary_person_df = primary_person_df
        self.units_df = units_df
        self.charges_df = charges_df
        self.endorse_df = endorse_df
        self.damages_df = damages_df
        self.restrict_df = restrict_df
        self.spark = SparkSession.builder.appName("DataLoader").getOrCreate()
        config_loader = ConfigLoader(config_path = "./config/config.yaml")
        self.config = config_loader.load_config()
        

    def male_death_analytics(self):
        try:
            """
            Compute the number of crashes where male deaths are greater than 2.
            
            Returns:
                int: Number of crashes with male deaths > 2.
            """

            male_deaths_df = self.primary_person_df.filter(self.primary_person_df["PRSN_GNDR_ID"] == "MALE")

            male_death_counts = male_deaths_df.groupBy("CRASH_ID").agg(
                F.sum("DEATH_CNT").alias("total_male_deaths")
            )
            crashes_with_male_deaths_gt_2 = male_death_counts.filter(male_death_counts["total_male_deaths"] > 2)
            result_df = pd.DataFrame([{"Query": "crashes_with_male_deaths_gt_2", "Count": crashes_with_male_deaths_gt_2.count()}])

            return result_df
        except Exception as e:
            print(f"Error in male_death_analytics: {e}")
            return None

    def two_wheelers_analytics(self):
        try:
            """
            Compute the number of two-wheelers booked for crashes.
            
            Returns:
                int: Number of two-wheelers involved in crashes.
            """
            relevant_categories = ["PEDALCYCLIST", "MOTORIZED CONVEYANCE"]
            
            two_wheelers_updated_df = self.units_df[self.units_df["UNIT_DESC_ID"].isin(relevant_categories)]

            two_wheelers_updated_count = two_wheelers_updated_df.count()
            two_wheelers_updated_count_df = pd.DataFrame({
                'Query': ['two_wheelers_updated_count'],
                'Count': [two_wheelers_updated_count]
            })
            return two_wheelers_updated_count_df
        except Exception as e:
            print(f"Error in two_wheelers_analytics: {e}")
            return None
        

    def vehicle_make_analytics(self):
        try:
            """
            Compute the top 5 vehicle makes in crashes where the driver died and airbags did not deploy.
            """
            filtered_person_df = self.primary_person_df[ 
                (self.primary_person_df['DEATH_CNT'] > 0) & 
                (self.primary_person_df['PRSN_AIRBAG_ID'] == 'NOT DEPLOYED')
            ]

            relevant_crashes = filtered_person_df[['CRASH_ID', 'UNIT_NBR']]

            merged_df = self.units_df.join(relevant_crashes, on=['CRASH_ID', 'UNIT_NBR'], how='inner')

            vehicle_make_counts = merged_df.groupBy('VEH_MAKE_ID').count()

            top_5_vehicle_makes = vehicle_make_counts.orderBy('count', ascending=False).limit(5)
            
            return top_5_vehicle_makes
        except Exception as e:
            print(f"Error in vehicle_make_analytics: {e}")
            return None

    def hit_and_run_analytics(self):
        try:
            """
            Compute the number of vehicles with valid licenses involved in hit-and-run incidents.
            """
            hit_and_run_vehicles = self.units_df[self.units_df['VEH_HNR_FL'] == 'Y']
            hit_and_run_vehicles = hit_and_run_vehicles.toPandas()

            self.endorse_df = self.endorse_df.toPandas()
            valid_license_vehicles = pd.merge(hit_and_run_vehicles, self.endorse_df, on=['CRASH_ID', 'UNIT_NBR'])
            
            valid_license_vehicles = valid_license_vehicles[ 
                (valid_license_vehicles['DRVR_LIC_ENDORS_ID'] != 'NONE') & 
                (valid_license_vehicles['DRVR_LIC_ENDORS_ID'] != 'UNLICENSED')
            ]
            
            number_of_valid_vehicles = valid_license_vehicles['UNIT_NBR'].count()
            number_of_valid_vehicles_df = pd.DataFrame({
                'Query': ['two_wheelers_updated_count'],
                'Count': [number_of_valid_vehicles]
            })
            return number_of_valid_vehicles_df
        except Exception as e:
            print(f"Error in hit_and_run_analytics: {e}")
            return None

    def non_female_accidents_analytics(self):
        try:
            """
            Compute the state with the highest number of accidents where females are not involved.

            Returns:
            str: The state with the highest number of accidents where females are not involved.
            """
            non_female_involved = self.primary_person_df[self.primary_person_df['PRSN_GNDR_ID'] != 'FEMALE']
            accident_counts_by_state = non_female_involved.groupBy('DRVR_LIC_STATE_ID').count()
            accident_counts_by_state = accident_counts_by_state.withColumnRenamed('count', 'Accident_Count')
            highest_accident_count = accident_counts_by_state.agg({"Accident_Count": "max"}).collect()[0]['max(Accident_Count)']

            highest_accident_state = accident_counts_by_state.filter(col('Accident_Count') == highest_accident_count)

            highest_accident_state = highest_accident_state.withColumnRenamed('DRVR_LIC_STATE_ID', 'State')

            result = highest_accident_state.collect()[0]
            state = result['State']
            count = result['Accident_Count']
            return state, count
        except Exception as e:
            print(f"Error in non_female_accidents_analytics: {e}")
            return None

    def clean_damage_column(self, df, col_name):
        try:
            """
            Clean the damage column to extract numeric damage level.
            """
            return df.withColumn(
            f"{col_name}_CLEAN",
            F.regexp_extract(F.col(col_name), r'DAMAGED (\d+)', 1).cast("float")
        )
        except Exception as e:
            print(f"Error in clean_damage_column: {e}")
            return None

    def get_count_distinct_crash_ids_no_damage_property(self):
        try:
            """
            Get count of distinct CRASH_IDs where No Damaged Property was observed,
            Damage Level > 4, and car has insurance.
            """
            units_df_1 = self.clean_damage_column(self.units_df, 'VEH_DMAG_SCL_1_ID')
            units_df_1 = self.clean_damage_column(units_df_1, 'VEH_DMAG_SCL_2_ID')

            units_filtered = units_df_1.filter(
                (F.col('VEH_DMAG_SCL_1_ID_CLEAN') > 4) | (F.col('VEH_DMAG_SCL_2_ID_CLEAN') > 4)
            ).filter(F.col('FIN_RESP_PROOF_ID').isNotNull())

            damages_filtered = self.damages_df.filter(self.damages_df['DAMAGED_PROPERTY'] == 'NONE')

            merged_df = units_filtered.join(damages_filtered, on='CRASH_ID', how='inner')

            distinct_crash_count = merged_df.select('CRASH_ID').distinct().count()

            distinct_crash_count_df = pd.DataFrame({
                'Query': ['two_wheelers_updated_count'],
                'Count': [distinct_crash_count]
            })

            return distinct_crash_count_df
        except Exception as e:
            print(f"Error in get_count_distinct_crash_ids_no_damage_property: {e}")
            return None

    def get_top_injury_vehicles(self):
        try:
            """
            Determines the Top 3rd to 5th VEH_MAKE_IDs contributing to the largest number of injuries including death.
            """
            units_df_2 = self.units_df.select('VEH_MAKE_ID', 'TOT_INJRY_CNT', 'DEATH_CNT')
            units_df_2 = units_df_2.fillna({'TOT_INJRY_CNT': 0, 'DEATH_CNT': 0})

            units_df_2 = units_df_2.withColumn('TOTAL_INJURIES', col('TOT_INJRY_CNT') + col('DEATH_CNT'))

            injury_summary = units_df_2.groupBy('VEH_MAKE_ID').agg(
                spark_sum('TOTAL_INJURIES').alias('TOTAL_INJURIES')
            )

            sorted_injury_summary = injury_summary.orderBy(col('TOTAL_INJURIES').desc())

            top_3_to_5 = sorted_injury_summary.limit(5).collect()[2:5]  

            for row in top_3_to_5:
                print(row)

            return top_3_to_5
        except Exception as e:
            print(f"Error in get_top_injury_vehicles: {e}")
            return None

    def body_style_ethnic_group_analytics(self):
        try:
            """
            Compute the top ethnic group for each vehicle body style.
            """
            merged_df = self.units_df.join(self.primary_person_df, on=['CRASH_ID', 'UNIT_NBR'], how='inner')
            grouped_df = merged_df.groupBy('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID').count()

            window_spec = Window.partitionBy('VEH_BODY_STYL_ID').orderBy(grouped_df['count'].desc())

            ranked_df = grouped_df.withColumn('rank', rank().over(window_spec))

            top_ethnic_groups = ranked_df.filter(ranked_df['rank'] == 1).drop('rank')

            return top_ethnic_groups
        except Exception as e:
            print(f"Error in body_style_ethnic_group_analytics: {e}")
            return None

    def top_zip_codes_with_alcohol_crashes(self, top_n=5):
        try:
            """
            Compute the top N zip codes with the highest number of alcohol-related crashes.
            """
            alcohol_related_crashes = self.primary_person_df.filter(
            (self.primary_person_df['PRSN_ALC_SPEC_TYPE_ID'].isNotNull()) & 
            (self.primary_person_df['DRVR_ZIP'].isNotNull())
        )
            zip_code_counts = alcohol_related_crashes.groupBy('DRVR_ZIP').count()

            top_zip_codes = zip_code_counts.orderBy(F.col('count').desc()).limit(top_n)
            
            return top_zip_codes
        except Exception as e:
            print(f"Error in top_zip_codes_with_alcohol_crashes: {e}")
            return None

    def top_vehicle_makes_for_speeding(self):
        try:
            self.charges_df = self.charges_df.toPandas()
            self.units_df = self.units_df.toPandas()
            self.primary_person_df = self.primary_person_df.toPandas()
            speeding_offenses = self.charges_df[self.charges_df['CHARGE'].str.contains('SPEED', case=False, na=False)]
        
            licensed_drivers = self.primary_person_df[(
                self.primary_person_df['DRVR_LIC_CLS_ID'].isin(['CLASS A', 'CLASS B', 'CLASS C', 'CLASS A AND M', 'CLASS B AND M', 'CLASS C AND M', 'OTHER/OUT OF STATE'])) |
                (self.primary_person_df['DRVR_LIC_TYPE_ID'].isin(['DRIVER LICENSE', 'COMMERCIAL DRIVER LIC.']))
            ]
            
            top_colors = self.units_df['VEH_COLOR_ID'].value_counts().head(10).index.tolist()
            
            top_states = self.primary_person_df['DRVR_LIC_STATE_ID'].value_counts().head(25).index.tolist()  

            merged_df = pd.merge(self.units_df, licensed_drivers, on=['CRASH_ID', 'UNIT_NBR'])
            merged_df = pd.merge(merged_df, speeding_offenses, on='CRASH_ID')
            
            filtered_df = merged_df[
                (merged_df['VEH_COLOR_ID'].isin(top_colors)) & 
                (merged_df['DRVR_LIC_STATE_ID'].isin(top_states))
            ]

            vehicle_make_counts = filtered_df['VEH_MAKE_ID'].value_counts().head(5)
            
            return vehicle_make_counts
        except Exception as e:
            print(f"Error in top_vehicle_makes_for_speeding: {e}")
            return None
