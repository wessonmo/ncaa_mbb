import pandas as pd

teams = pd.read_csv('kaggle\\data\\Teams.csv', header = 0)[['TeamID','ncaa_id']]

seasons = pd.read_csv('kaggle\\data\\Seasons.csv', header = 0)[['Season','DayZero']]
seasons.loc[:,'DayZero'] = pd.to_datetime(seasons.DayZero)

games = pd.read_csv('kaggle\\data\\RegularSeasonDetailedResults.csv', header = 0).drop(['WLoc'], axis = 1)
games = games.loc[games.Season >= 2009,]

games = pd.merge(games, seasons, how = 'left', on = 'Season')\
        .merge(teams, how = 'left', left_on = 'WTeamID', right_on = 'TeamID')\
        .merge(teams, how = 'left', left_on = 'LTeamID', right_on = 'TeamID')
games.loc[:,'game_date'] = games.apply(lambda x: x.DayZero + pd.DateOffset(days = x.DayNum), axis = 1)
games.drop(['Season','DayNum','DayZero','TeamID_x','TeamID_y','WTeamID','LTeamID'], axis = 1, inplace = True)
games.rename(index = str, columns = {'ncaa_id_x': 'WTeamID', 'ncaa_id_y': 'LTeamID'}, inplace = True)

mod = games.copy()
mod.loc[:,'low_id'] = mod.apply(lambda x: min(x.WTeamID,x.LTeamID), axis = 1)
mod.loc[:,'high_id'] = mod.apply(lambda x: max(x.WTeamID,x.LTeamID), axis = 1)

for index, row in mod.iterrows():
    for team in ['W','L']:
        new = 'low_' if row[team + 'TeamID'] == row.low_id else 'high_'
        for col in [x[1:] for x in games.columns if (x[0] == team) & (x[1:] != 'TeamID')]:
            mod.set_value(index, new + col, row[team + col])

drop_cols = [x for x in mod.columns if x[0] in ['W','L']]
mod.drop(drop_cols, axis = 1, inplace = True)

for tm, opp in zip(['low','high'],['high','low']):
    mod.loc[:,tm + '_teff'] = mod.apply(lambda x: x[tm + '_Score']/(2*x[tm + '_FGA'] + x[tm + '_FGA3'] + x[tm + '_FTA']), axis = 1)
    mod.loc[:,tm + '_efg'] = mod.apply(lambda x: (x[tm + '_Score'] - x[tm + '_FTM'])/(2*x[tm + '_FGA'] + x[tm + '_FGA3']), axis = 1)
                                  
    mod.loc[:,tm + '_ptapm'] = mod.apply(lambda x: (2*x[tm + '_FGA'] + x[tm + '_FGA3'] + x[tm + '_FTA'])
                                    /(40 + 5*x.NumOT), axis = 1)

    mod.loc[:,tm + '_astp'] = mod.apply(lambda x: x[tm + '_Ast']/x[tm + '_FGM'], axis = 1)
    
    mod.loc[:,tm + '_blkp'] = mod.apply(lambda x: x[tm + '_Blk']/x[opp + '_FGA'], axis = 1)
    
    mod.loc[:,tm + '_rbp'] = mod.apply(lambda x: x[tm + '_OR']/(x[tm + '_OR'] + x[opp + '_DR']), axis = 1)
  
drop_cols = set(x for x in mod.columns if x.split('_')[-1] not in ['teff','efg','ptapm','astp','blkp','rbp'])\
            - set(['game_date','low_id','high_id'])
mod.drop(drop_cols, axis = 1, inplace = True)
    
with open('csv\\games_stats.csv', 'wb') as csvfile:
    mod.to_csv(csvfile, header = True, index = False)