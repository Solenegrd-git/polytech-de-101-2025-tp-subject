import json
from datetime import datetime, date

import duckdb
import pandas as pd

today_date = datetime.now().strftime("%Y-%m-%d")
current_date = date.today()
con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)


def create_consolidate_tables():
    with open("data/sql_statements/create_consolidate_tables.sql") as fd:
        statements = fd.read()
        for statement in statements.split(";"):
            print(statement)
            con.execute(statement)

def consolidate_city_data():

    ###### Paris Data ######
    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data_paris = json.load(fd)

    raw_paris_df = pd.json_normalize(data_paris)
    raw_paris_df["nb_inhabitants"] = None

    city_paris = raw_paris_df[[
        "code_insee_commune",
        "nom_arrondissement_communes",
        "nb_inhabitants"
    ]].rename(columns={
        "code_insee_commune": "id",
        "nom_arrondissement_communes": "name"
    }).drop_duplicates()

    city_paris["created_date"] = current_date
    ###### Nantes Data ######
    # Nantes is one city → static INSEE code
    city_nantes = pd.DataFrame([{
        "id": "44109",
        "name": "Nantes",
        "nb_inhabitants": None,
        "created_date": current_date
    }])

    ###### FINAL MERGE ######
    city_data_df = pd.concat([city_paris, city_nantes], ignore_index=True)

    print(city_data_df)
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_CITY SELECT * FROM city_data_df;")


def consolidate_station_data():

    data_paris = {}
    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data_paris = json.load(fd)

    raw_data_df = pd.json_normalize(data_paris)
    station_data_df = pd.DataFrame()
# ID, CODE, NAME, CITY_NAME, CITY_CODE, ADDRESS, LONGITUDE, LATITUDE, STATUS, CREATED_DATE, CAPACITTY
# pour on genere un id unique 

    for index, row in raw_data_df.iterrows():
            station_data_df.at[index, "id"] = f"{row['code_insee_commune']}_{row['stationcode']}"

    station_data_df["code"] = raw_data_df["stationcode"].astype(str)
    station_data_df["name"] = raw_data_df["name"]
    station_data_df["city_name"] = raw_data_df["nom_arrondissement_communes"]
    station_data_df["city_code"] = raw_data_df["code_insee_commune"]
    station_data_df["address"] = None
    station_data_df["longitude"] = raw_data_df["coordonnees_geo.lon"]
    station_data_df["latitude"] = raw_data_df["coordonnees_geo.lat"]
    station_data_df["status"] = raw_data_df["is_installed"]
    station_data_df["created_date"] = current_date
    station_data_df["capacitty"] = raw_data_df["capacity"]
    station_data_df.drop_duplicates(subset=["id"], inplace=True)
    print(station_data_df)

    # insertion des donnees de nantes 

    data_nantes = {}
    with open(f"data/raw_data/{today_date}/nantes_realtime_bicycle_data.json") as fd:
        data_nantes = json.load(fd)

    raw_nantes_df = pd.json_normalize(data_nantes["results"])
    station_data_nantes_df = pd.DataFrame()
    for index, row in raw_nantes_df.iterrows():
            station_data_nantes_df.at[index, "id"] = f"44109_{row['number']}"
    
    station_data_nantes_df["code"] = raw_nantes_df["number"].astype(str)
    station_data_nantes_df["name"] = raw_nantes_df["name"]
    station_data_nantes_df["city_name"] = "Nantes"
    station_data_nantes_df["city_code"] = "44109"
    station_data_nantes_df["address"] = raw_nantes_df["address"]
    station_data_nantes_df["longitude"] = raw_nantes_df["position.lon"]
    station_data_nantes_df["latitude"] = raw_nantes_df["position.lat"]
    station_data_nantes_df["status"] = None
    station_data_nantes_df["created_date"] = current_date
    station_data_nantes_df["capacitty"] = raw_nantes_df["bike_stands"]
    station_data_nantes_df.drop_duplicates(subset=["id"], inplace=True)
    print(station_data_nantes_df)
    station_data_df = pd.concat([station_data_df, station_data_nantes_df], ignore_index=True)

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM station_data_df;")

# fonction pour consolider les donnees de disponibilite des stations ( statsion_id , bicycle_docks_available, bicycle_available, last_statement_date, created_date)
def consolidate_station_statement_data():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data_paris = {}
    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data_paris = json.load(fd)

    raw_data_df = pd.json_normalize(data_paris)
    station_paris_statement_data_df = pd.DataFrame()

    for index, row in raw_data_df.iterrows():
            station_paris_statement_data_df.at[index, "station_id"] = f"{row['code_insee_commune']}_{row['stationcode']}"

    station_paris_statement_data_df["bicycle_docks_available"] = raw_data_df["numdocksavailable"]
    station_paris_statement_data_df["bicycle_available"] = raw_data_df["numbikesavailable"]
    station_paris_statement_data_df["last_statement_date"] = pd.to_datetime(raw_data_df["duedate"], errors='coerce')
    station_paris_statement_data_df["created_date"] = current_date
    print(station_paris_statement_data_df)

    data_nantes = {}
    with open(f"data/raw_data/{today_date}/nantes_realtime_bicycle_data.json") as fd:
        data_nantes = json.load(fd)

    raw_nantes_df = pd.json_normalize(data_nantes["results"])
    station_nantes_statement_data_df = pd.DataFrame()
    for index, row in raw_nantes_df.iterrows():
            station_nantes_statement_data_df.at[index, "station_id"] = f"44109_{row['number']}"

    station_nantes_statement_data_df["bicycle_docks_available"] = raw_nantes_df["available_bike_stands"]
    station_nantes_statement_data_df["bicycle_available"] = raw_nantes_df["available_bikes"]
    station_nantes_statement_data_df["last_statement_date"] = pd.to_datetime(raw_nantes_df["last_update"], errors='coerce')
    station_nantes_statement_data_df["created_date"] = current_date
    print(station_nantes_statement_data_df)
    station_statement_data_df = pd.concat([station_paris_statement_data_df, station_nantes_statement_data_df], ignore_index=True)
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM station_statement_data_df;")


def consolidate_population_data():
    """Clean, normalize and update population numbers."""

    con = duckdb.connect(
        database="data/duckdb/mobility_analysis.duckdb",
        read_only=False
    )

    with open(f"data/raw_data/{today_date}/insee_population_data.json") as fd:
        data_population = json.load(fd)

    raw_population_df = pd.json_normalize(data_population)

    # normalize codgeo: strip, then left-pad to 5
    population_df = pd.DataFrame()
    population_df["id"] = (
        raw_population_df["codgeo"]
        .astype(str)
        .str.strip()
        .str.zfill(5)
    )

    population_df["nb_inhabitants"] = (
        raw_population_df["p21_pop"]
        .astype("Int64")
        .fillna(0)
    )

    # register table in duckdb
    con.register("population_df", population_df)

    # UPDATE using JOIN, converting CONSOLIDATE_CITY.ID to VARCHAR
    con.execute("""
        UPDATE CONSOLIDATE_CITY AS c
        SET NB_INHABITANTS = CAST(p.nb_inhabitants AS INT)
        FROM population_df p
        WHERE CAST(c.ID AS VARCHAR) = p.id;
    """)

    print("Population updated.")
