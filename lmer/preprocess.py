import pandas as pd
import re

school_divs = pd.read_csv('csv\\school_divs.csv', header = 0)[['season','school_id','division']]

school_info = pd.read_csv('csv\\school_info.csv', header = 0)[['season','school_id','city','arena','capacity']]
school_info.loc[:,'capacity'] = school_info.loc[:,'capacity'].fillna(0)

cities = pd.read_csv('csv\\cities.csv', header = 0)
arenas = pd.read_csv('csv\\arenas.csv', header = 0)

school_info = pd.merge(school_info, cities, how = 'left', on = 'city')\
                .rename(index = str, columns = {'latitude': 'city_lat', 'longitude': 'city_long'})\
                .merge(arenas, how = 'left', on = 'arena').rename(index = str, columns = {'latitude': 'arena_lat',
                      'longitude': 'arena_long'})

games = pd.read_csv('csv\\games.csv', header = 0)
games = games.loc[pd.isnull(games.opp_id) == False]

data = games
match_cols = ['season','school_id']
for table in [school_divs, school_info]:
    remain_cols = set(table.columns) - set(match_cols)
    for side in ['school','opp']:
        data = pd.merge(data, table, how = 'left', left_on = ['season', side + '_id'], right_on = match_cols)
        for col in remain_cols:
                data.rename(index = str, columns = {col: side + '_' + col}, inplace = True)
    data.rename(index = str, columns = {'school_id_x': 'school_id'}, inplace = True)
    data.drop(['school_id_y'], axis = 1, inplace = True)

sites = pd.read_csv('csv\\sites.csv', header = 0)

data = pd.merge(data, sites, how = 'left', on = 'site').rename(index = str, columns = {'latitude': 'site_lat', 'longitude': 'site_long'})

for var in ['lat','long']:
    for side in ['school','opp']:
        data.loc[:,side + '_' + var] = data.apply(lambda x: x[side + '_city_' + var] if pd.isnull(x[side + '_arena_' + var])\
                    else x[side + '_arena_' + var], axis = 1)
        
    data.loc[:,'site_' + var] = data.apply(lambda x: x['school_' + var] if x.location == 'Home' else x['opp_' + var]
                                    if x.location == 'Away' else x['site_' + var], axis = 1)
            
data.loc[:,'perc_capacity'] = data.apply(lambda x: x.attend/x.school_capacity
                                 if (x.location == 'Home') and (x.school_capacity > 0) else
                                     x.attend/x.opp_capacity if (x.location == 'Away') and (x.opp_capacity > 0)
                                     else None, axis = 1)

data.loc[:,'game_id'] = data.apply(lambda x: re.sub('/','_',x.game_date) + '.' + str(int(min(x.school_id,x.opp_id))) + '.'
                         + str(int(max(x.school_id,x.opp_id))), axis = 1)

comb = pd.DataFrame()
column_changes = ['_'.join(x.split('_')[1:]) for x in data.columns if x.split('_')[0] == 'school']
for side in ['school','opp']:
    data.loc[:,'location'] = data.apply(lambda x: 'Neutral' if x.location == 'Neutral' else x.location
                if side == 'school' else list(set(['Home','Away']) - set([x.location]))[0], axis = 1)
    for col in column_changes:
        data.rename(index = str, columns = {side + '_' + col: 'off_' + col}, inplace = True)
        data.rename(index = str, columns = {list(set(['school','opp']) - set([side]))[0] + '_' + col: 'def_' + col}, inplace = True)
        
    comb = pd.concat([comb, data]).sort_values(['game_id','off_id'])

comb.iloc[0]