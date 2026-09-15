import os 
os.environ["KAGGLE_CONFIG_DIR"] = "."

from kaggle.api.kaggle_api_extended import KaggleApi


api = KaggleApi()
api.authenticate()


dataset_name = 'akrambelha/synthetic-banking-dataset-csv-sql-sqlite'

api.dataset_download_files(dataset_name, path='data/', unzip=True)
