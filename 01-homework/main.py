from pipeline.ingest_data import process_data


def db_params():
    return {
        'user': 'root',
        'password': 'root',
        'host': 'postgres',
        'port': 5432,
        'db': 'trip_data'
    }


def trip_data():
    url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz"
    file_path = 'files/data.csv.gz'
    table_name = 'green_tripdata'
    process_data(file_path, db_params(), table_name, url)


def zone_lookup():
    url = "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
    file_path = 'files/data.csv'
    table_name = 'zone_lookup'
    process_data(file_path, db_params(), table_name, url)


if __name__ == '__main__':
    trip_data()
    zone_lookup()
