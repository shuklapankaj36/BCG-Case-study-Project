## vehicle-related data {BCG Case Study}
This project is a comprehensive data analysis and processing pipeline built using Apache Spark and Python. It aims to provide insights into various aspects of traffic accidents and vehicle-related data.

##  Table of Contents
Table of Contents
About
Requirements
Project File Structure
Installing
Usage
Troubleshooting

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

BCG-End-to-End-Project/
├── data/
│   ├── input/
│   │   ├── primary_person.csv
│   │   ├── units.csv
│   │   ├── charges.csv
│   │   ├── endorse.csv
│   │   ├── damages.csv
│   │   └── restrict.csv
│   └── output/
│       ├── male_deaths_result.json
│       ├── two_wheelers_result.json
│       ├── vehicle_make_result.json
│       ├── hit_and_run_result.json
│       ├── non_female_accidents.json
│       ├── top_3_to_5_vehicles.json
│       ├── body_style_ethnic_group_result.json
│       ├── top_zip_codes_with_alcohol.json
│       └── top_vehicle_makes_for_speeding.json
├── src/
│   ├── analytics.py
│   ├── process.py
│   ├── result_saver.py
│   ├── utils.py
│   └── __init__.py
├── main.py
├── requirements.txt
├── README.md
└── LICENSE


## Installing
  Install the required libraries using pip:
    pip install -r requirements.txt


## Usage
Run the main script using Python:
python main.py

## Troubleshooting

If you encounter any issues during the execution of the project, please refer to the following troubleshooting steps:

*   Check the data sources and ensure they are in the correct format.
*   Verify that the required libraries are installed correctly.
*   Check the output files for any errors or inconsistencies.
*   Refer to the Apache Spark and Python documentation for any specific errors or issues. 