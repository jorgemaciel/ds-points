# %%
import argparse
import datetime

import pandas as pd
import sqlalchemy
from sqlalchemy import exc

from tqdm import tqdm


def import_query(path):
    with open(path, 'r') as open_file:
        return open_file.read()


def date_range(start, stop):
    dt_start = datetime.datetime.strptime(start, '%Y-%m-%d')
    dt_stop = datetime.datetime.strptime(stop, '%Y-%m-%d')
    dates = []
    while dt_start <= dt_stop:
        dates.append(dt_start.strftime('%Y-%m-%d'))
        dt_start += datetime.timedelta(days=1)
    return dates


def ingest_data(query, table, dt):
    query_fmt = query.format(date=dt)
    df = pd.read_sql(query_fmt, ORIGIN_ENGINE)

    with TARGET_ENGINE.connect() as con:
        try:
            state = f"DELETE FROM {table} WHERE dtRef = '{dt}';"
            con.execute(sqlalchemy.text(state))
            con.commit()
        except exc.OperationalError as err:
            print('Tabela não existe, criando...')
    
    df.to_sql(table, TARGET_ENGINE, index=False, if_exists='append')


#%%

now = datetime.datetime.now().strftime('%Y-%m-%d')

parser = argparse.ArgumentParser()
parser.add_argument('--feature_store', '-f', help='Nome da Feature Store', type=str)
parser.add_argument('--start', '-s', help='Data de inicio', default=now, type=str)
parser.add_argument('--stop', '-p', help='Data de fim', default=now, type=str)
args = parser.parse_args()

ORIGIN_ENGINE = sqlalchemy.create_engine('sqlite:///../../data/database.db')
TARGET_ENGINE = sqlalchemy.create_engine('sqlite:///../../data/feature_store.db')

query = import_query(f'{args.feature_store}.sql')
dates = date_range(args.start, args.stop)

for i in tqdm(dates):
    ingest_data(query, args.feature_store, 1)
