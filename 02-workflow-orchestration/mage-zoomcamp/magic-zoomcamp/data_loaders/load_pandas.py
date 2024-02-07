import pandas as pd
import os


if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/'
    year=2020
    months=[10, 11, 12]
    dfs = []
    dtype_dict = {
        'VendorID': 'category',
        'passenger_count': 'float32',
        'trip_distance': 'float32',
        'RatecodeID': 'category',
        'store_and_fwd_flag': 'category',
        'PULocationID': 'int16',
        'DOLocationID': 'int16',
        'payment_type': 'category',
        'fare_amount': 'float32',
        'extra': 'float32',
        'mta_tax': 'float32',
        'tip_amount': 'float32',
        'tolls_amount': 'float32',
        'improvement_surcharge': 'float32',
        'total_amount': 'float32',
        'congestion_surcharge': 'float32'
    }
    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
    for month in months:
        month_str = f"{month:02d}"
        file_name = f"green_tripdata_{year}-{month_str}.csv.gz"
        file_url = os.path.join(url, file_name)

        df = pd.read_csv(file_url, compression='gzip', dtype=dtype_dict, parse_dates=parse_dates)
        dfs.append(df)


    final_df = pd.concat(dfs, ignore_index=True)
    return final_df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
