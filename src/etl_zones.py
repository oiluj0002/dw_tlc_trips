import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from io import BytesIO

def extract(url):
    '''Função que extrai os dados brutos da fonte'''
    response = requests.get(url)
    data = BytesIO(response.content)
    
    print('Iniciando extração')
    
    return data

def transform(data):
    zones = pd.read_csv(data)
    
    print('Dados de Locais extraídos')
    
    return zones

def load(zones):
    credentials = service_account.Credentials.from_service_account_file('./keyfile.json')
    client = bigquery.Client(project='dw-tlc-trip',
                             credentials=credentials,
                             location='US')
    dataset = client.create_dataset(dataset='bronze', exists_ok=True)
    table = dataset.table('zones')
    
    client.load_table_from_dataframe(dataframe=zones, destination=table)

def main():
    url = 'https://d37ci6vzurychx.cloudfront.net//misc/taxi_zone_lookup.csv'
    data = extract(url)
    trips = transform(data)
    load(trips)

if __name__ == '__main__':
    main()