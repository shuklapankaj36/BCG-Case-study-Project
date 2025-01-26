class ResultsSaver:
    """ 
    The ResultSaver class takes a DataSaver object as input and provides a method to save the results of the analytics to JSON files.
    The save_results method takes multiple DataFrames as input and saves them to JSON files based on the configuration.

    """

    def __init__(self, data_saver):
        self.data_saver = data_saver

    def save_results(self, male_deaths_result, body_style_ethnic_group_result, two_wheelers_result, vehicle_make_result, hit_and_run_result, distinct_crash_count, result_top_3_to_5, top_zip_codes_result, top_vehicle_makes_result, state_non_female):
        self.data_saver.save_to_json(male_deaths_result, "male_deaths_result")
        self.data_saver.save_to_json(body_style_ethnic_group_result, "body_style_ethnic_group_result")
        self.data_saver.save_to_json(two_wheelers_result, "two_wheelers_result")
        self.data_saver.save_to_json(vehicle_make_result, "vehicle_make_result")
        self.data_saver.save_to_json(hit_and_run_result, "hit_and_run_result")
        self.data_saver.save_to_json(distinct_crash_count, "distinct_crash_count")
        self.data_saver.save_to_json(result_top_3_to_5, "top_3_to_5_vehicles")
        self.data_saver.save_to_json(top_zip_codes_result, "top_zip_codes_with_alcohol")
        self.data_saver.save_to_json(top_vehicle_makes_result, "top_vehicle_makes_for_speeding")
        self.data_saver.save_to_json(state_non_female, "non_female_accidents")