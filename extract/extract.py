from s3fs import S3FileSystem
from dotenv import dotenv_values
from multiprocessing import Pool
import concurrent.futures
import requests
import os
import csv
import pandas as pd
from sqlalchemy import create_engine, text

API_URL = "https://data-eng-plants-api.herokuapp.com/plants/"
EXTRACTED_DATA_LOCATION = "/tmp/extracted_download"
CSV_Path = "/tmp/extracted_download/plant.csv"
EXTRACT_BUCKET = "t3-extraction-bucket"
    
def get_plant_data(plant_id):
    """calling api for data"""
    response = requests.get(f"{API_URL}{plant_id}")
    if response.status_code == 200:
        return response.json()
    return None


def folder_creation():
    """creation of folders to store downloaded items"""
    if not os.path.exists(EXTRACTED_DATA_LOCATION):
        os.makedirs(EXTRACTED_DATA_LOCATION)


def csv_creation(plant_list: list):
    """converts list of dict to csv file"""
    keys = ["botanist","cycle","images","last_watered","name","origin_location","plant_id","recording_taken","scientific_name","soil_moisture","sunlight","temperature"]
    with open(CSV_Path, 'w', newline='') as output:
        writer = csv.DictWriter(output, keys) 
        writer.writeheader()
        writer.writerows(plant_list)

def connect_to_s3(config: dict):
    """Connect to bucket"""
    fs = S3FileSystem(key=config["ACCESS_KEY"], secret=config["SECRET_KEY"])
    return fs
    
def csv_to_s3(instance):
    """uploads csv to bucket"""
    s3_bucket_path=f"s3://{EXTRACT_BUCKET}/plant.csv"
    print(s3_bucket_path)
    print(CSV_Path)
    instance.upload(CSV_Path,s3_bucket_path)

def connect_database(df, config):
    print(f"Connecting to database via engine with schema raw")
    engine = create_engine(f'postgresql+psycopg2://{config["DATABASE_USERNAME"]}:{config["DATABASE_PASSWORD"]}@{config["DATABASE_IP"]}:{config["DATABASE_PORT"]}/{config["DATABASE_NAME"]}')
    with engine.connect() as conn:
        df.to_sql("raw_data", conn, schema=config["SCHEMA_NAME"], if_exists="replace", index=False)


def handler(event, context):
    # config = dotenv_values()
    config = os.environ
    plant_list =[]
    plant_id = 0
    with concurrent.futures.ThreadPoolExecutor() as executor:
         plant_list = executor.map(get_plant_data, [plant_id for plant_id in range(0,51)])
         plant_list = [plant_data for plant_data in plant_list if plant_data is not None]
    # for plant_id in range(0,51):
    #     data = get_plant_data(plant_id)
    #     if data is not None:
    #         plant_list.append(data)
    folder_creation()
    if len(plant_list) > 0:
        csv_creation(plant_list)
        df = pd.read_csv(CSV_Path)
        # s3 = connect_to_s3(config)
        # csv_to_s3(s3)
        connect_database(df, config)


# if __name__=="__main__":
#     handler(1,2)