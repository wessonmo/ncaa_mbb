import pandas as pd
from geopy.distance import vincenty

games = pd.read_csv('csv\\games.csv', header = 0)
games = games.loc[(games.school_id < games.opp_id),].drop(['school_score','opp_score','ot','attend'], axis = 1)

school_geo = pd.read_csv('csv\\school_loc.csv', header = 0).drop(['arena_comb','city'], axis = 1)
opp_geo = school_geo.copy().rename(index = str, columns = {'school_id': 'opp_id'})
offcampus_geo = pd.read_csv('csv\\game_loc.csv', header = 0).drop(['site_sec'], axis = 1)

for df, var in zip([school_geo, opp_geo, offcampus_geo], [['season','school_id'], ['season','opp_id'], ['site']]):
    games = pd.merge(games, df, how = 'left', on = var)\
        .rename(index = str, columns = {'latitude': var[-1] + '_' + 'lat', 'longitude': var[-1] + '_' + 'long'})
        
distances = games.copy()
for var in ['school','opp']:
    for loc_type in [list(set(['school','opp']) - set([var]))[0] + '_id_','site_']:
        var_list = [var + '_id_lat', var + '_id_long', loc_type + 'lat', loc_type + 'long']
        if loc_type == 'site_':
            temp = games.loc[(games[var_list].isnull().sum(axis = 1) == 0),var_list].drop_duplicates()
        else:
            temp = games.loc[(games[var_list].isnull().sum(axis = 1) == 0) & pd.isnull(games.site),var_list].drop_duplicates()
        temp.loc[:,var + '_' + loc_type + 'dist'] = temp.apply(lambda x:
            vincenty((x[var + '_id_lat'], x[var + '_id_long']), (x[loc_type + 'lat'], x[loc_type + 'long'])).miles, axis = 1)
            
        distances = pd.merge(distances, temp, how = 'left', on = var_list)
    
distances.loc[:,'school_dist'] = distances.apply(lambda x: x.school_site_dist if pd.isnull(x.site) == False
    else x.school_opp_id_dist if x.location == 'Away' else 0, axis = 1)
distances.loc[:,'opp_dist'] = distances.apply(lambda x: x.opp_site_dist if pd.isnull(x.site) == False
    else x.opp_school_id_dist if x.location == 'Home' else 0, axis = 1)

distances = distances.loc[distances[['school_dist','opp_dist']].isnull().sum(axis = 1) < 2,
                          ['game_date','school_id','opp_id','school_dist','opp_dist']]

with open('csv\\school_dist.csv', 'wb') as csvfile:
    distances.to_csv(csvfile, header = True, index = False)