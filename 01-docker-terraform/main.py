from pipeline.ingest_data import process_data

url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
file_path = 'files/yellow_tripdata_2021-01.csv'
table_name = 'taxi_trips'
db_params = {
    'user': 'root',
    'password': 'root',
    'host': 'postgres',
    'port': 5432,
    'db': 'ny_taxi'
}


process_data(file_path, db_params, table_name, url)
