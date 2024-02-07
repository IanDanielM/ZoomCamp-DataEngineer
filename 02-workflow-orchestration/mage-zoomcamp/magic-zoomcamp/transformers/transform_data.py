if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    data = data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]
    
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
    
    data.columns = [col.lower() for col in data.columns]
    data.columns = data.columns.str.replace(' ', '_')
    return data
    


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output['vendorid'].isin(['1', '2']).all(), "vendor_id contains invalid values"
    
    # Ensure passenger_count is greater than 0
    assert (output['passenger_count'] > 0).all(), "There are rows with passenger_count <= 0"
    
    # Ensure trip_distance is greater than 0
    assert (output['trip_distance'] > 0).all(), "There are rows with trip_distance <= 0"
