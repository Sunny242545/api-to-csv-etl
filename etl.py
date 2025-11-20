import requests #type: ignore
import pandas as pd #type: ignore
import yaml #type: ignore
import logging
import json

logging.basicConfig(filename = 'etl-csv.log', filemode = 'w', level = logging.INFO , format = '%(asctime)s,%(lineno)d,%(message)s')


def load_config(path = "config.yml"):
    with open(path, 'r') as f:
        return yaml.safe_load(f)
    


class APItoCSV:
    def __init__(self, config_file = "config.yml"):
        logging.info('Loading the config file....')
        self.props = load_config(config_file)
        self.url = self.props.get('url')
    
    def read_from_api(self):
        logging.info('calling API to get data...')
        self.url= self.url
        self.response = requests.get(self.url)
        self.json_data = self.response.json()
        return self.response.json()
    
    def transform(self, data):
        
        self.data = pd.DataFrame(data)
        self.cols = self.props.get('useful_columns')
        logging.info(f"Filtering columns: {self.cols}")
        logging.info(self.cols)

        #flatted the nested data
        self.data['city'] = self.data['address'].apply(lambda x :x['city'])
        self.data['zipcode'] = self.data['address'].apply(lambda x :x['zipcode'])
        self.data['company_name'] = self.data['company'].apply(lambda x :x['name'])
    

        self.data = self.data[self.cols]

        return self.data

    def load(self, data):
        self.output_file = self.props.get('output_file')
        data.to_csv(self.output_file, index = False)

    
    

users = APItoCSV('config.yml')
data = users.read_from_api()
df = users.transform(data)
users.load(df)