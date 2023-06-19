"""Module to download plant measurement data from an S3 bucket, clean it and then upload to another S3 bucket."""

import os
from dotenv import load_dotenv
import pandas as pd
import json
from datetime import datetime
from s3fs import S3FileSystem
from sqlalchemy import create_engine, engine

DOWNLOAD_BUCKET = "t3-extraction-bucket"
UPLOAD_BUCKET = "t3-transform-bucket"
TMP_DIRECTORY = "/tmp/"
CSV_OUTPUT_DIRECTORY = f"{TMP_DIRECTORY}csv"
SCHEMA = "raw"
TABLE_NAME = "raw_data"

def get_db_connection(config: dict) -> engine:
    """Connect to an rds using sqlalchemy engines"""

    db_uri = f'postgresql+psycopg2://{config["DATABASE_USERNAME"]}:{config["DATABASE_PASSWORD"]}@{config["DATABASE_IP"]}:{config["DATABASE_PORT"]}/{config["DATABASE_NAME"]}'

    engine = create_engine(db_uri)

    return engine


def get_raw_data(config: dict) -> pd.DataFrame:
    """Use an sqlalchemy engine to connect to an rds and read data from rds."""

    engine = get_db_connection(config)

    conn = engine.connect()

    raw_data = pd.read_sql_table(TABLE_NAME, conn, schema=SCHEMA)

    conn.close()

    engine.dispose()

    return raw_data


def construct_botanist_df(dataframe: pd.Series) -> pd.DataFrame:
    """Create a new data frame from flattening botanist dictionary."""

    columns = ["email", "name"]
    rows = []

    for row in dataframe.values:
        row = row.replace("'", "\"")
        json_value = json.loads(row)
        rows.append(list(json_value.values())[0:2])

    botanist_df = pd.DataFrame(rows, columns=columns)

    botanist_df = botanist_df.drop_duplicates()

    botanist_df["first_name"] = botanist_df["name"].apply(lambda x: x.split()[0])
    botanist_df["last_name"] = botanist_df["name"].apply(lambda x: x.split()[1])

    botanist_df = botanist_df.drop("name", axis=1)

    return botanist_df


def clean_plant_df(plant_dataframe: pd.DataFrame) -> pd.DataFrame:
    """clean plant related data by flattening columns"""

    plant_dataframe["scientific_name"] =  plant_dataframe["scientific_name"].str.replace("""["'[\]]""", "", regex=True)

    plant_dataframe["origin_location"] = plant_dataframe["origin_location"].apply(lambda x: json.loads(x.replace("'", "\"")))

    plant_dataframe["origin_city"] = plant_dataframe["origin_location"].apply(lambda x: x[-1].split("/")[1].replace("_", " "))

    plant_dataframe["origin_continent"] = plant_dataframe["origin_location"].apply(lambda x: x[-1].split("/")[0])

    plant_dataframe = plant_dataframe.drop("origin_location", axis=1)

    return plant_dataframe


def clean_measurement_df(measurement_data: pd.DataFrame, botanist_data: pd.DataFrame, plant_data: pd.DataFrame) -> pd.DataFrame:
    """clean measurement related data by flattening columns and rounding float columns.
    Also put date column in standard format."""

    measurement_data["email"] = measurement_data["botanist"].apply(lambda x: list(json.loads(x.replace("'", "\"")).values())[0])

    measurement_data = measurement_data.drop("botanist", axis=1)

    measurement_data["botanist_id"] = measurement_data["email"].apply(lambda x: botanist_data[botanist_data["email"] == x].index.values.astype(int)[0])

    measurement_data = measurement_data.drop("email", axis=1)

    measurement_data["last_watered"] = measurement_data["last_watered"].apply(lambda x: datetime.strptime(x, "%a, %d %B %Y %H:%M:%S %Z"))

    measurement_data["soil_moisture"] = measurement_data["soil_moisture"].apply(lambda x: round(x, 1))

    measurement_data["temperature"] = measurement_data["temperature"].apply(lambda x: round(x, 1))

    return measurement_data


def upload_clean_data(config: dict=os.environ, bucket: str=UPLOAD_BUCKET, folder: str="", directory: str=CSV_OUTPUT_DIRECTORY):
    """Connect to S3 and upload clean data to a bucket"""

    fs = S3FileSystem(key=config["ACCESS_KEY"], secret=config["SECRET_KEY"])

    path = bucket + folder

    files = os.listdir(f"{directory}")

    for file in files:
        try:
            fs.upload(f"{directory}/{file}", path)
        except Exception as err:
            print(err.args[0])
    
    print("Upload Success")


def handler(event, context):

    pd.options.mode.chained_assignment = None

    load_dotenv()

    df = get_raw_data(os.environ)

    botanist_df = construct_botanist_df(df["botanist"])

    plant_df = df[["plant_id", "cycle", "name", "scientific_name", "origin_location"]]

    measurement_df = df[["botanist", "plant_id" ,"recording_taken", "last_watered", "soil_moisture", "temperature"]]

    plant_df = clean_plant_df(plant_df)

    measurement_df = clean_measurement_df(measurement_df, botanist_df, plant_df)

    if not os.path.exists(f"{CSV_OUTPUT_DIRECTORY}"):
        os.mkdir(f"{CSV_OUTPUT_DIRECTORY}")

    botanist_df.to_csv(f"{CSV_OUTPUT_DIRECTORY}/botanist.csv", index=True, index_label="botanist_id")

    plant_df.to_csv(f"{CSV_OUTPUT_DIRECTORY}/plant.csv", index=False)

    measurement_df.to_csv(f"{CSV_OUTPUT_DIRECTORY}/measurement.csv", index=True, index_label="id")

    upload_clean_data()