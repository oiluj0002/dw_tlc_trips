import requests
import pyarrow.parquet as pq
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
    '''Função que transforma os dados'''
    df = pq.read_table(data)

    trips = df.to_pandas()

    trips['passenger_count'] = trips['passenger_count'].astype('Int64')
    trips['RatecodeID'] = trips['RatecodeID'].astype('Int64')
    trips['payment_type'] = trips['payment_type'].astype('Int64')
    trips = trips[['VendorID', 
                   'tpep_pickup_datetime', 
                   'tpep_dropoff_datetime',
                   'passenger_count',
                   'trip_distance',
                   'PULocationID',
                   'DOLocationID',
                   'RatecodeID',
                   'store_and_fwd_flag',
                   'payment_type',
                   'fare_amount',
                   'extra',
                   'mta_tax',
                   'improvement_surcharge',
                   'tip_amount',
                   'tolls_amount',
                   'congestion_surcharge',
                   'Airport_fee']]
    
    print('Dados de Viagens extraídos')
    
    return trips

def load(trips):
    credentials = service_account.Credentials.from_service_account_file('./keyfile.json')
    client = bigquery.Client(project='dw-tlc-trip',
                             credentials=credentials,
                             location='US')
    dataset = client.create_dataset(dataset='bronze', exists_ok=True)
    table = dataset.table('trips')
    
    client.load_table_from_dataframe(dataframe=trips, destination=table)

def main():
    url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-05.parquet'
    data = extract(url)
    trips = transform(data)
    load(trips)

if __name__ == '__main__':
    main()