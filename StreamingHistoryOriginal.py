#converts original .json file to .csv
import os
import zipfile
import pandas as pd

json_file_path = os.path.join('/Users/ca/Documents/data_eng_projects/', 'StreamingHistory0.json')
csv_file_path = os.path.join('/Users/ca/Documents/data_eng_projects/', 'StreamingHistory0.csv')

data = pd.read_json(json_file_path)
data.to_csv(csv_file_path, index=False)

data.head(), csv_file_path
