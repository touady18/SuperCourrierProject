# SuperCourier - Mini ETL Pipeline
# Starter code for the Data Engineering mini-challenge

import sqlite3
import pandas as pd
import numpy as np
import json
import logging
from datetime import datetime, timedelta
import random
import os

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('supercourier_mini_etl')

# Constants
DB_PATH = 'supercourier_mini.db'
WEATHER_PATH = 'weather_data.json'
OUTPUT_PATH = 'deliveries.csv'

# 1. FUNCTION TO GENERATE SQLITE DATABASE (you can modify as needed)
def create_sqlite_database():
    """
    Creates a simple SQLite database with a deliveries table
    """
    logger.info("Creating SQLite database...")
    
    # Remove database if it already exists
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    
    # Connect to database
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Create deliveries table
    cursor.execute('''
    CREATE TABLE deliveries (
        delivery_id INTEGER PRIMARY KEY,
        pickup_datetime TEXT,
        package_type TEXT,
        delivery_zone TEXT,
        recipient_id INTEGER
    )
    ''')
    
    # Available package types and delivery zones
    package_types = ['Small', 'Medium', 'Large', 'X-Large', 'Special']
    delivery_zones = ['Urban', 'Suburban', 'Rural', 'Industrial', 'Shopping Center']
    
    # Generate 1000 random deliveries
    deliveries = []
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)  # 3 months
    
    for i in range(1, 1001):
        # Random date within last 3 months
        timestamp = start_date + timedelta(
            seconds=random.randint(0, int((end_date - start_date).total_seconds()))
        )
        
        # Random selection of package type and zone
        package_type = random.choices(
            package_types, 
            weights=[25, 30, 20, 15, 10]  # Relative probabilities
        )[0]
        
        delivery_zone = random.choice(delivery_zones)
        
        # Add to list
        deliveries.append((
            i,  # delivery_id
            timestamp.strftime('%Y-%m-%d %H:%M:%S'),  # pickup_datetime
            package_type,
            delivery_zone,
            random.randint(1, 100)  # fictional recipient_id
        ))
    
    # Insert data
    cursor.executemany(
        'INSERT INTO deliveries VALUES (?, ?, ?, ?, ?)',
        deliveries
    )
    
    # Commit and close
    conn.commit()
    conn.close()
    
    logger.info(f"Database created with {len(deliveries)} deliveries")
    return True

# 2. FUNCTION TO GENERATE WEATHER DATA
def generate_weather_data():
    """
    Generates fictional weather data for the last 3 months
    """
    logger.info("Generating weather data...")
    
    conditions = ['Sunny', 'Cloudy', 'Rainy', 'Windy', 'Snowy', 'Foggy']
    weights = [30, 25, 20, 15, 5, 5]  # Relative probabilities
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)
    
    weather_data = {}
    
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        weather_data[date_str] = {}
        
        # For each day, generate weather for each hour
        for hour in range(24):
            # More continuity in conditions
            if hour > 0 and random.random() < 0.7:
                # 70% chance of keeping same condition as previous hour
                condition = weather_data[date_str].get(str(hour-1), 
                                                      random.choices(conditions, weights=weights)[0])
            else:
                condition = random.choices(conditions, weights=weights)[0]
            
            weather_data[date_str][str(hour)] = condition
        
        current_date += timedelta(days=1)
    
    # Save as JSON
    with open(WEATHER_PATH, 'w', encoding='utf-8') as f:
        json.dump(weather_data, f, ensure_ascii=False, indent=2)
    
    logger.info(f"Weather data generated for period {start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')}")
    return weather_data

# 3. EXTRACTION FUNCTIONS (to be completed)
def extract_sqlite_data():
    """
    Extracts delivery data from SQLite database
    """
    logger.info("Extracting data from SQLite database...")
    
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT * FROM deliveries"
    df = pd.read_sql(query, conn)
    conn.close()

    # I want to hava DATE and HOUR separated to be able to get the weather 
    # Creation of two new features 
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'], errors='coerce')
    df['pickup_date'] = df['pickup_datetime'].dt.strftime('%Y-%m-%d')
    df['pickup_hour'] = df['pickup_datetime'].dt.hour.astype(str)
    
    logger.info(f"Extraction complete: {len(df)} records")
    return df

def load_weather_data():
    """
    Loads weather data from JSON file
    """
    logger.info("Loading weather data...")
    
    with open(WEATHER_PATH, 'r', encoding='utf-8') as f:
        weather_data = json.load(f)
    
    logger.info(f"Weather data loaded for {len(weather_data)} days")
    return weather_data

# 4. TRANSFORMATION FUNCTIONS (to be completed by participants)

def enrich_with_weather(df, weather_data):
    """
    Enriches the DataFrame with weather conditions
    """
    logger.info("Enriching with weather data...")
    
    # Don't need it anymore, already did it
    # Convert date column to datetime
    # df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
    
    # Here I transform the weather data to df

    weather_records = []
    for date_str, hours in weather_data.items():
        for hour_str, condition in hours.items():
            weather_records.append({
                'pickup_date': date_str,
                'pickup_hour': hour_str,
                'WeatherCondition': condition
            })
    
    weather_df = pd.DataFrame(weather_records)
    # Merge the data
    merged_df = pd.merge(df, weather_df, on=['pickup_date', 'pickup_hour'], how='left')
    
    logger.info("Weather enrichment complete")
    return merged_df


def transform_data(df_deliveries, weather_data):
    """
    Main data transformation function
    To be completed by participants
    """
    logger.info("Transforming data...")
    # Added by Dyhia 17-04-2024

    # 1.Enrich deliveries data with weather
    new_df = enrich_with_weather(df_deliveries, weather_data)

    # 2 Calculate the needed data
    new_df['Distance'] = np.round(np.random.uniform(1, 50, size=len(new_df)), 2)
    new_df['Actual_Delivery_Time'] = new_df['Distance'] * np.random.uniform(0.8, 1.5, size=len(new_df)) + 30
    new_df['Actual_Delivery_Time'] = new_df['Actual_Delivery_Time'].round(2)
    new_df['Weekday'] = new_df['pickup_datetime'].dt.day_name()

    parcel = {
        'Small': 1,
        'Medium': 1.2,
        'Large': 1.5,
        'X-Large': 2,
        'Special': 2.5
    }
    zone = {
        'Urban': 1.2,
        'Suburban': 1,
        'Rural': 1.3,
        'Industrial': 0.9,
        'Shopping Center': 1.4
    }
    weather = {
        'Sunny': 1,
        'Cloudy': 1.05,
        'Rainy': 1.2,
        'Windy': 1.1,
        'Snowy': 1.8,
        'Foggy': 1.3
    }
    new_df['Base_Theoretical_Time'] = 30 + new_df['Distance'] * 0.8
    new_df['Adjustment'] = new_df['package_type'].map(parcel) * \
                               new_df['delivery_zone'].map(zone) * \
                               new_df['WeatherCondition'].map(weather).fillna(1)
    new_df['Adjusted_Time'] = new_df['Base_Theoretical_Time'] * new_df['Adjustment']
    new_df['Delay_Threshold'] = new_df['Adjusted_Time'] * 1.2
    new_df['Status'] = np.where(new_df['Actual_Delivery_Time'] > new_df['Delay_Threshold'], 'Delayed', 'On-time')

    new_df = new_df.dropna()

    needed_col = ['delivery_id', 'pickup_datetime', 'Weekday', 'pickup_date','pickup_hour', 'package_type', 'Distance',
        'delivery_zone', 'WeatherCondition', 'Actual_Delivery_Time', 'Status']

    new_df = new_df[needed_col]
    
    return new_df  # Return transformed DataFrame

# 5. LOADING FUNCTION (to be completed)
def save_results(df):
    """
    Saves the final DataFrame to CSV
    """
    logger.info("Saving results...")
    
    # TODO: Add your data validation code here
    
    # Save to CSV
    df.to_csv(OUTPUT_PATH, index=False)
    
    logger.info(f"Results saved to {OUTPUT_PATH}")
    return True

# MAIN FUNCTION
def run_pipeline():
    """
    Runs the ETL pipeline end-to-end
    """
    try:
        logger.info("Starting SuperCourier ETL pipeline")
        
        # Step 1: Generate data sources
        create_sqlite_database()
        weather_data = generate_weather_data()
        
        # Step 2: Extraction
        df_deliveries = extract_sqlite_data()


        #TO DELETE
        # added Dyhia ->
        print("******************* print some DELIVERIES DATA")
        print(df_deliveries.head())
        #TO DELETE
        data = load_weather_data()
        df_test = pd.DataFrame(data)
        # print some weather data
        print("******************* print some DELIVERIES DATA")
        print(df_test.head())
        #TO DELETE
        df_test2 = enrich_with_weather(df_deliveries,data)
        # print data transformed :

        print("******************* print weather added to deliveries")
        print(df_test2.head())

        # Step 3: Transformation
        df_transformed = transform_data(df_deliveries, weather_data)


        # Step 4: Loading
        save_results(df_transformed)
        
        logger.info("ETL pipeline completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error during pipeline execution: {str(e)}")
        return False

# Main entry point
if __name__ == "__main__":
    run_pipeline()
