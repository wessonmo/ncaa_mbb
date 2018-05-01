import pandas as pd
# import numpy as np

def teams():
	index_df = pd.read_csv('csv\\team_index.csv')[['school_name','season','season_id','school_id']]

	for var_name in ['rosters','coaches','schedules','facilities']:
		file_loc = 'csv\\{0}.csv'.format(var_name)
		file_df = pd.read_csv(file_loc)
		third_col = file_df.columns[2]
		exist = file_df.loc[~pd.isnull(file_df[third_col]),['season_id','school_id']].drop_duplicates()

		index_df = pd.merge(index_df, exist, how = 'left', on = ['season_id','school_id'], indicator = var_name)
		index_df.loc[:,var_name] = index_df[var_name].apply(lambda x: 0 if x == 'both' else 1)

	missing = index_df.drop(['school_id','season_id'], axis = 1).groupby('season').sum().reset_index()

	print('Missing')
	print('\n')
	print(missing)
	print('\n')
	for var_name in ['rosters','coaches','schedules','facilities']:
		if missing[var_name].sum() > 0:
			miss_list = [x for x in index_df.loc[index_df.coaches == 1,['season','school_name']].head(10).apply(lambda x: ' - '.join([str(y) for y in x]), axis = 1)]
			print('{0}: {1}'.format(var_name, miss_list))


# teams()