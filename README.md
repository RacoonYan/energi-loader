# energi-loader

Fetches data from https://www.energidataservice.dk and loads it into an sqlite database.

## Setup

Install requirements with: `pip install -r requirements.txt`

## Running 

The ETL script takes 4 arguments:
| # | Desc                          | Example                    |
|---|-------------------------------|----------------------------|
| 1 | Path to sqllite db.           | data/energi.sqlite         |
| 2 | First date to fetch data for. | 2023-01-01                 |
| 3 | Last date to fetch date for.  | 2023-12-18                 |
| 4 | List of energi DBs.           | CO2Emis,DeclarationGridmix |

Each dataset will be loaded into a separete table with the datasets name as table name.

Example command:

```sh
python src/main/python/energi_loader.py data/energi.sqlite 2023-01-01 2023-12-18 CO2Emis,DeclarationGridmix,DeclarationProduction 
```

## Datasets of interest

The following datasets are a good satrting point to explore:

1. CO2Emis
2. DeclarationProduction
3. DeclarationGridmix

For more information of the datasets above
https://www.energidataservice.dk/datasets 