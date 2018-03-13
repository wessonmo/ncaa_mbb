import pandas as pd

teams = pd.read_csv('kaggle\\data\\Teams.csv', header = 0)[['TeamID','ncaa_id']]

seasons = pd.read_csv('kaggle\\data\\Seasons.csv', header = 0)[['Season','DayZero']]
seasons.loc[:,'DayZero'] = pd.to_datetime(seasons.DayZero)

games = pd.read_csv('kaggle\\data\\RegularSeasonDetailedResults.csv', header = 0)
games = games.loc[games.Season >= 2009,]

games = pd.merge(games, seasons, how = 'left', on = 'Season')\
        .merge(teams, how = 'left', left_on = 'WTeamID', right_on = 'TeamID')\
        .merge(teams, how = 'left', left_on = 'LTeamID', right_on = 'TeamID')
games.loc[:,'game_date'] = games.apply(lambda x: x.DayZero + pd.DateOffset(days = x.DayNum), axis = 1)
games.drop(['Season','DayNum','DayZero','TeamID_x','TeamID_y','WTeamID','LTeamID'], axis = 1, inplace = True)
games.rename(index = str, columns = {'ncaa_id_x': 'WTeamID', 'ncaa_id_y': 'LTeamID'}, inplace = True)

mod = games.copy()
mod.loc[:,'home_id'] = mod.apply(lambda x: x.WTeamID if x.WLoc == 'H' else x.LTeamID
                        if x.WLoc == 'A' else min(x.WTeamID,x.LTeamID), axis = 1)
mod.loc[:,'away_id'] = mod.apply(lambda x: x.LTeamID if x.WLoc == 'H' else x.WTeamID
                        if x.WLoc == 'A' else max(x.WTeamID,x.LTeamID), axis = 1)

for index, row in mod.iterrows():
    for team in ['W','L']:
        new = 'home_' if row[team + 'TeamID'] == row.home_id else 'away_'
        for col in [x[1:] for x in games.columns if (x[0] == team) & (x[1:] != 'TeamID')]:
            mod.set_value(index, new + col, row[team + col])

mod.loc[:,'loc'] = mod.home_Loc.apply(lambda x: 'H' if x in ['H','A'] else 'N')
drop_cols = [x for x in mod.columns if x[0] in ['W','L']]
mod.drop(drop_cols + ['home_Loc','away_Loc'], axis = 1, inplace = True)

for tm, opp in zip(['home','away'],['away','home']):
    mod.loc[:,tm + '_teff'] = mod.apply(lambda x: x[tm + '_Score']/(2*x[tm + '_FGA'] + x[tm + '_FGA3'] + x[tm + '_FTA']), axis = 1)
    mod.loc[:,tm + '_efg'] = mod.apply(lambda x: (x[tm + '_Score'] - x[tm + '_FTM'])/(2*x[tm + '_FGA'] + x[tm + '_FGA3']), axis = 1)
                                  
    mod.loc[:,tm + '_pta_min'] = mod.apply(lambda x: (2*x[tm + '_FGA'] + x[tm + '_FGA3'] + x[tm + '_FTA'])
                                    /(40 + 5*x.NumOT), axis = 1)

    mod.loc[:,tm + '_astp'] = mod.apply(lambda x: x[tm + '_Ast']/x[tm + '_FGM'], axis = 1)
    
    mod.loc[:,tm + '_blkp'] = mod.apply(lambda x: x[tm + '_Blk']/x[opp + '_FGA'], axis = 1)
    
    mod.loc[:,tm + '_orbp'] = mod.apply(lambda x: x[tm + '_OR']/(x[tm + '_OR'] + x[opp + '_DR']), axis = 1)
    mod.loc[:,tm + '_drbp'] = mod.apply(lambda x: x[tm + '_DR']/(x[tm + '_DR'] + x[opp + '_OR']), axis = 1)
    
comb = pd.DataFrame()
for tm, opp in zip(['home','away'],['away','home']):
    var_list = [x for x in mod.columns if (x[:4] == tm) and (x.split('_')[-1] in ['teff','efg','min','astp','blkp','orbp','drbp'])]
    temp = mod.loc[:,['game_date', tm + '_id', opp + '_id'] + var_list]\
            .rename(index = str, columns = {tm + '_id': 'school_id', opp + '_id': 'opp_id'})
    for col in var_list:
        temp.rename(index = str, columns = {col: '_'.join(col.split('_')[1:])}, inplace = True)
    comb = pd.concat([comb, temp])
    
with open('csv\\games_stats.csv', 'wb') as csvfile:
    comb.to_csv(csvfile, header = True, index = False)