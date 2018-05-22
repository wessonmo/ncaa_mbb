import pandas as pd
import sqlalchemy

engine = sqlalchemy.create_engine('postgresql://postgres:@localhost:5432/ncaa_mbb')

rosters = pd.read_sql_table('rosters', engine, schema = 'raw_data')

rosters.loc[:,'ncaa_id'] = rosters.player_id.apply(lambda x: True if not pd.isnull(x) else False)

len_miss = len(rosters.loc[~rosters.ncaa_id])
max_id = rosters.loc[rosters.ncaa_id].player_id.max()

rosters.loc[~rosters.ncaa_id,'player_id'] = [x + max_id for x in range(len_miss)]

engine.execute('create schema if not exists imputed_data;')
rosters.to_sql('rosters', engine, schema = 'imputed_data', if_exists = 'append', index = False)