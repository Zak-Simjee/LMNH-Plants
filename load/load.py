import os
import csv
from s3fs import S3FileSystem
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from datetime import datetime

DOWNLOAD_BUCKET = "t3-transform-bucket"
TMP_DIRECTORY = "/tmp/"
CURRENT_DATE = datetime.now().date()
DAILY_SCHEMA = "daily"
HISTORICAL_SCHEMA = "historical"


def download_data(config: dict=os.environ, bucket: str=DOWNLOAD_BUCKET, folder: str="", directory: str=TMP_DIRECTORY):

    fs = S3FileSystem(key=config["ACCESS_KEY"], secret=config["SECRET_KEY"])
    path = bucket + folder

    files = fs.ls(path)

    for file in files:
        fs.download(file, f"{directory}")


def get_db_connection(daily: bool, config: dict=os.environ):
    """establishes connection to database"""

    schema = DAILY_SCHEMA if daily else HISTORICAL_SCHEMA

    try:
        connection = psycopg2.connect(user = config["DATABASE_USERNAME"], \
                                      password = config["DATABASE_PASSWORD"],\
                                      host = config["DATABASE_IP"], \
                                      port = config["DATABASE_PORT"], \
                                      database = config["DATABASE_NAME"], \
                                      options=f"-c search_path={schema}") 
        return connection
    except Exception as err:
        print("Error connecting to database.")
        print(err)


def drop_tables(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("drop table if exists measurement CASCADE;")
            cur.execute("drop table if exists botanist;")
            cur.execute("drop table if exists plant;")
            conn.commit()
    except Exception as err: 
        print(err)  


def create_tables(conn):
    try:
        with conn.cursor() as cur:

            cur.execute("""create table if not exists botanist(
            botanist_id INT not null,
            email VARCHAR(320) not null UNIQUE,
            first_name TEXT not null,
            last_name TEXT not null,
            PRIMARY KEY(botanist_id)
            );""")

            cur.execute("""create table if not exists plant (
	        plant_id INT not null,
            cycle VARCHAR(200),
            name TEXT not null,
            scientific_name VARCHAR(200),
            origin_city TEXT not null,
            origin_continent TEXT not null,
            PRIMARY KEY(plant_id)
            );""")

            cur.execute("""create table if not exists measurement (
	        measurement_id INT generated always as identity,
            plant_id INT not null,
	        recording_taken TIMESTAMP not null,
	        last_watered TIMESTAMP not null,
            soil_moisture FLOAT,
            temperature FLOAT,
            botanist_id INT not null,
            PRIMARY KEY(measurement_id),
            CONSTRAINT fk_plant
                FOREIGN KEY(plant_id) 
	                REFERENCES plant(plant_id),
            CONSTRAINT fk_botanist
                FOREIGN KEY(botanist_id) 
	                REFERENCES botanist(botanist_id)
                
            );""")
            conn.commit()
    except Exception as err: 
        print(err)


def insert_plant(conn):
    cur = conn.cursor()
    with open(f'{TMP_DIRECTORY}plant.csv') as plant_csv:
        contents = csv.DictReader(plant_csv)
        for row in contents:
            cur.execute("INSERT INTO plant (plant_id, cycle, name, scientific_name, origin_city, origin_continent)\
                        VALUES (%s, %s, %s, %s, %s, %s)\
                        ON CONFLICT DO NOTHING", (row["plant_id"], row["cycle"], row["name"],
                                                  row["scientific_name"], row["origin_city"], row["origin_continent"],))
        conn.commit()
    cur.close()


def insert_botanist(conn):
    cur = conn.cursor()
    with open(f'{TMP_DIRECTORY}botanist.csv') as botanist_csv:
        contents = csv.DictReader(botanist_csv)
        for row in contents:
            cur.execute("INSERT INTO botanist (botanist_id, email, first_name, last_name)\
                         VALUES (%s, %s, %s, %s)\
                         ON CONFLICT DO NOTHING", (row["botanist_id"], row["email"],
                                                   row["first_name"], row["last_name"]))
        conn.commit()
    cur.close()


def insert_measurement(conn):
    cur = conn.cursor()
    with open(f'{TMP_DIRECTORY}measurement.csv') as measurement_csv:
        contents = csv.DictReader(measurement_csv)
        for row in contents:
            cur.execute("INSERT INTO measurement (plant_id, recording_taken, last_watered, soil_moisture, temperature, botanist_id)\
                        VALUES (%s, %s, %s, %s, %s, %s)\
                        ON CONFLICT DO NOTHING", (row["plant_id"], row["recording_taken"], row["last_watered"],
                                                  row["soil_moisture"], row["temperature"], row["botanist_id"],))
        conn.commit()
    cur.close()


def create_and_insert(conn):
    create_tables(conn)
    insert_plant(conn)
    insert_botanist(conn)
    insert_measurement(conn)


def check_daily_data(conn):

    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("SELECT recording_taken FROM measurement\
                 LIMIT 1")

    recording_date = cur.fetchone()["recording_taken"].date()

    if recording_date == CURRENT_DATE:
        return True
    else:
        return False


def update_daily_tables():
    conn = get_db_connection(daily=True)

    create_and_insert(conn)

    if not check_daily_data(conn):
        drop_tables(conn)
        create_and_insert(conn)
        conn.close()
    
    conn.close()


def update_historical_tables():
    conn = get_db_connection(daily=False)
    create_and_insert(conn)
    conn.close()

def handler(event, context):
    load_dotenv()
    download_data()
    update_historical_tables()
    update_daily_tables()