import pandas as pd
import re
from geopy.distance import vincenty

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
games = games.loc[(pd.isnull(games.opp_id) == False) & (games.school_id == games[['school_id','opp_id']].min(axis = 1))]
games.loc[:,'game_id'] = games.apply(lambda x: re.sub('/','',x.game_date) + str(int(x.school_id)) + str(int(x.opp_id)), axis = 1)
games.drop_duplicates(['game_id']).reset_index(drop = True).sort_values(['season','game_id'], inplace = True)
games.loc[:,'ot'] = games.loc[:,'ot'].fillna(0)

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

distances = pd.DataFrame(columns = ['school_lat','school_long','site_lat','site_long'])
for side, loc in zip(['school','opp'],['Home','Away']):
    var_list = [side + '_lat', side + '_long', 'site_lat','site_long']
    temp = data.loc[(data[var_list].isnull().sum(axis = 1) == 0) & (data.location != loc),var_list]
    if side == 'opp':
        temp.rename(index = str, columns = {'opp_lat': 'school_lat', 'opp_long': 'school_long'}, inplace = True)
    distances = pd.concat([distances,temp]).drop_duplicates().reset_index(drop = True)

distances.loc[:,'geo_dist'] = distances.apply(lambda x: vincenty((x.school_lat,x.school_long), (x.site_lat, x.site_long)).miles, axis = 1)

for side in ['school','opp']:
    temp = distances.rename(index = str, columns = {'school_lat': side + '_lat', 'school_long': side + '_long'})
    data = pd.merge(data, temp, how = 'left', on = [side + '_lat',side + '_long', 'site_lat', 'site_long'])\
                .rename(index = str, columns = {'geo_dist': side + '_dist'})
    for col in ['_city','_arena','_city_lat','_city_long','_arena_lat','_arena_long','_lat','_long']:
        data.drop([side + col], axis = 1, inplace = True)
        
data.drop(['site','site_lat','site_long'], axis = 1, inplace = True)

data.loc[:,'perc_capacity'] = data.apply(lambda x: x.attend/x.school_capacity
                                 if (x.location == 'Home') and (x.school_capacity > 0) else
                                     x.attend/x.opp_capacity if (x.location == 'Away') and (x.opp_capacity > 0)
                                     else None, axis = 1)

comb = pd.DataFrame()
column_changes = ['_'.join(x.split('_')[1:]) for x in data.columns if (x.split('_')[0] == 'school')
                    and ('_'.join(x.split('_')[1:]) != 'score')]
for side in ['school','opp']:
    temp = data.copy()
    temp.loc[:,'location'] = temp.apply(lambda x: 'Neutral' if x.location == 'Neutral' else x.location
                if side == 'school' else list(set(['Home','Away']) - set([x.location]))[0], axis = 1)
    temp.rename(index = str, columns = {side + '_score': 'pts'}, inplace = True)
    temp.drop([list(set(['school','opp']) - set([side]))[0] + '_score'], axis = 1, inplace = True)
    for col in column_changes:
        temp.rename(index = str, columns = {side + '_' + col: 'off_' + col}, inplace = True)
        temp.rename(index = str, columns = {list(set(['school','opp']) - set([side]))[0] + '_' + col: 'def_' + col}, inplace = True)
    
    comb = pd.concat([comb, temp]).sort_values(['game_id','off_id']).reset_index(drop = True)

comb.loc[:,'ot'] = comb.loc[:,'ot'].fillna(0)

for side, loc in zip(['off','def'],['Home','Away']):
    comb.loc[:,side + '_dist'] = comb.apply(lambda x: 0 if (pd.isnull(x[side + '_dist']) == True) and (x.location == loc)
                                    else x[side + '_dist'], axis = 1)
    comb.loc[:,side + '_id'] = comb.apply(lambda x: str(x.season) + '_' + str(int(x[side + '_id'])), axis = 1)
    
with open('csv\\lmer_data.csv', 'ab') as csvfile:
    comb.to_csv(csvfile, header = True, index = False)