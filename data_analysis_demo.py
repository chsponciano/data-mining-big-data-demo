from glob import glob
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import sqlite3


def create_connector():
    return create_engine('sqlite:///csv_database.db')

def initialize_dataframe(files : list, chunksize : int = 10000, index_start  = 0):
    csv_database = create_connector()

    for file in files:
        for df in pd.read_csv(file, chunksize=chunksize, sep=";", encoding='Latin-1', iterator=True):
            
            df = df.rename(columns = {c: c.replace(' ', '') for c in df.columns})
            df['DATADAEXTRACAO']        = pd.to_datetime(df['DATADAEXTRACAO'],      errors='coerce')
            df['DATADAFILIACAO']        = pd.to_datetime(df['DATADAFILIACAO'],      errors='coerce')
            df['DATADOPROCESSAMENTO']   = pd.to_datetime(df['DATADOPROCESSAMENTO'], errors='coerce')
            df['DATADADESFILIACAO']     = pd.to_datetime(df['DATADADESFILIACAO'],   errors='coerce') 
            df['HORADAEXTRACAO']        = pd.to_datetime(df['HORADAEXTRACAO'],      errors='coerce') 
            df['DATADOCANCELAMENTO']    = pd.to_datetime(df['DATADOCANCELAMENTO'],  errors='coerce') 
            df['DATADAREGULARIZACAO']   = pd.to_datetime(df['DATADAREGULARIZACAO'], errors='coerce') 

            df.index += index_start 
            df.to_sql('data', csv_database, if_exists='append')
            index_start  = df.index[-1] + 1

            print(f'| index: {index_start}')

def get_cvs_files(folder : str) -> list:
    return [csv for csv in glob(folder + '**/*.csv', recursive=True)]

def data_visualization():
    csv_database = create_connector()

    df = pd.read_sql_query('''SELECT SIGLADOPARTIDO, COUNT(*) as NUM_FILIADOS 
                            FROM data 
                            GROUP BY SIGLADOPARTIDO 
                            ORDER BY SIGLADOPARTIDO''', csv_database)

    fig = plt.figure(figsize=(10, 7))
    plt.barh(df.SIGLADOPARTIDO, df.NUM_FILIADOS, color='blue')
    
    plt.ylabel('Siglas dos Partidos')
    plt.xlabel('Quantidade de filiados')
    plt.suptitle('Quantidade de filados por partidos')

    plt.show()