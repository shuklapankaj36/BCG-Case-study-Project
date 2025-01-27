## vehicle-related data {BCG Case Study}
This project is a comprehensive data analysis and processing pipeline built using Apache Spark and Python. It aims to provide insights into various aspects of traffic accidents and vehicle-related data.

##  Table of Contents
* Table of Contents
* About
* Requirements
* Project File Structure
* Installing
* Usage
* Troubleshooting
* Built Usage

##  About
Use 6 csv files in the input and develop your approach to perform below analytics.

## Requirements

1.	Analytics 1: Find the number of crashes (accidents) in which number of males killed are greater than 2?
2.	Analysis 2: How many two wheelers are booked for crashes? 
3.	Analysis 3: Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.
4.	Analysis 4: Determine number of Vehicles with driver having valid licences involved in hit and run? 
5.	Analysis 5: Which state has highest number of accidents in which females are not involved? 
6.	Analysis 6: Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
7.	Analysis 7: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  
8.	Analysis 8: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
9.	Analysis 9: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
10.	Analysis 10: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)


## Project File Structure
The basic project structure is shown as below:

![image](https://github.com/user-attachments/assets/6704e593-0131-48fa-994b-cba391d99b80)


## Installing
  Install the required libraries using pip:

  
![image](https://github.com/user-attachments/assets/b1bd9048-5008-470e-9230-c83c8f7ac890)



## Usage
Run the main script using Python:


![image](https://github.com/user-attachments/assets/10badd73-f031-44ab-99cb-6f520e7b43f8)


## Output Files
The output files are stored in the `data/output` directory.

* `male_deaths_result.json`: Analysis 1
* `two_wheelers_result.json`: Analysis 2
* `vehicle_make_result.json`: Analysis 3
* `hit_and_run_result.json`: Analysis 4
* `non_female_accidents.json`: Analysis 5
* `top_3_to_5_vehicles.json`: Analysis 6
* `body_style_ethnic_group_result.json`: Analysis 7
* `top_zip_codes_with_alcohol.json`: Analysis 8
* `distinct_crash_count.json`: Analysis 9
* `top_vehicle_makes_for_speeding.json`: Analysis 10


## Data
The input data is stored in the `data/input` directory.


## Troubleshooting

If you encounter any issues during the execution of the project, please refer to the following troubleshooting steps:

*   Check the data sources and ensure they are in the correct format.
*   Verify that the required libraries are installed correctly.
*   Check the output files for any errors or inconsistencies.
*   Refer to the Apache Spark and Python documentation for any specific errors or issues.

  ## Built Usage
* Pyspark - Data Processing Framework
* Pandas - Data Analysis Library
* Jupyter Notebook - Data Analysis Tool
