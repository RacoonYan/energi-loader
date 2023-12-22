import energi_loader
import datetime
import pandas.api.types as ptypes

API_RESPONSE = {
        "total": 1465496,
        "limit": 4,
        "dataset": "CO2Emis",
        "records": [
            {
            "Minutes5UTC": "2023-12-20T12:00:00",
            "Minutes5DK": "2023-12-20T13:00:00",
            "PriceArea": "DK1",
            "CO2Emission": 94.000000
            }
        ]
    }

def test_to_dataframe():
    res = energi_loader.to_data_frame(API_RESPONSE)
    assert res is not None
    assert res.shape[0] == 1
 

def test_transform():
    day = datetime.date(2023,12,20)
    df = energi_loader.to_data_frame(API_RESPONSE)
    res = energi_loader.transform(df, day)
    assert res is not None
    assert res.shape[0] == 1
    assert "date" in df.columns
    assert ptypes.is_datetime64_any_dtype(df['Minutes5UTC'])
    assert ptypes.is_datetime64_any_dtype(df['Minutes5DK'])