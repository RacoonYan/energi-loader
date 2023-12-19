import requests
import datetime as dt
import pandas as pd
import os

INTERESTING_DATASETS = ["CO2Emis", "DeclarationProduction", "DeclarationGridmix", "CapacityPerMunicipality"]

def getDataset(dataset, day, limit=10):
    end = (dt.datetime.strptime(day, "%Y-%m-%d") + dt.timedelta(days=1)).strftime("%Y-%m-%d")
    try:
        response = requests.get(f"https://api.energidataservice.dk/dataset/{dataset}?start={day}T00:00&end={end}&limit={limit}")
        return response.json()
    except:
        raise e

def toDataframe(json_dict):
    if json_dict.get('records'):
        return pd.DataFrame.from_dict(json_dict['records'])
    
def getAsDataFrame(dataset, day, limit=10):
    return toDataframe(getDataset(dataset, day, limit))

def getAndSaveAsCsv(path, dataset, day, limit=10):
    file = os.path.join(path, f"{dataset}.csv") 
    df = toDataframe(getDataset(dataset, day, limit))
    if df is not None:
        os.makedirs(path, exist_ok=True)
        df.to_csv(file)
# # temporary for data exploration
for ds in INTERESTING_DATASETS:
    getAndSaveAsCsv(path="energi", dataset=ds, day="2023-01-01", limit=1000)