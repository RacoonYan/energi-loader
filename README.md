# energi-loader

## Setup

Install requirements with: `pip install -r requirements.txt`

## Running 

The ETL script takes 4 arguments:
| # | Desc                          | Example                    |
|---|-------------------------------|----------------------------|
| 1 | Path to sqllite db.           | data/energi.sqlite         |
| 2 | First date to fetch data for. | 2023-01-01                 |
| 3 | Last date to fetch date for.  | 2023-01-31                 |
| 4 | List of energi DBs.           | CO2Emis,DeclarationGridmix |

Each database will be loaded into a separete table with the database name as table name.

Example command:

```sh
python src/main/python/energi_loader.py data/energi.sqlite 2023-01-01 2023-01-31 CO2Emis,DeclarationGridmix 
```