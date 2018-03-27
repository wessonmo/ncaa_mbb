import pandas as pd

teams = pd.read_csv('kaggle\\data\\Teams.csv', header = 0).drop(['TeamName','FirstD1Season','LastD1Season'], axis = 1)

seasons = pd.read_csv('kaggle\\data\\Seasons.csv', header = 0).drop(['RegionW','RegionX','RegionY','RegionZ'], axis = 1)
seasons.loc[:,'DayZero'] = pd.to_datetime(seasons.DayZero)

ncaa = pd.read_csv('kaggle\\data\\NCAATourneyCompactResults.csv', header = 0).drop(['WScore','LScore'], axis = 1)

ncaa = pd.merge(ncaa.loc[ncaa.Season >= 2009], seasons, how = 'left', on = 'Season')
ncaa.loc[:,'game_date'] = ncaa.apply(lambda x: x.DayZero + pd.DateOffset(days = x.DayNum), axis = 1)
ncaa.drop(['Season','DayNum','DayZero'], axis = 1, inplace = True)

ncaa = pd.merge(ncaa, teams, how = 'left', left_on = 'WTeamID', right_on = 'TeamID')\
            .merge(teams, how = 'left', left_on = 'LTeamID', right_on = 'TeamID')\
            .drop(['TeamID_x','TeamID_y','WTeamID','LTeamID'], axis = 1)

ncaa.loc[:,'low_id'] = ncaa.apply(lambda x: min(x.ncaa_id_x,x.ncaa_id_y), axis = 1)
ncaa.loc[:,'high_id'] = ncaa.apply(lambda x: max(x.ncaa_id_x,x.ncaa_id_y), axis = 1)
ncaa.drop(['ncaa_id_x','ncaa_id_y'], axis = 1, inplace = True)

ncaa.loc[:,'ncaa'] = 1

with open('csv\\ncaa_tourn_games.csv', 'wb') as csvfile:
    ncaa.to_csv(csvfile, header = True, index = False)

conf = pd.read_csv('kaggle\\data\\ConferenceTourneyGames.csv', header = 0).drop(['ConfAbbrev'], axis = 1)

conf = pd.merge(conf.loc[conf.Season >= 2009], seasons, how = 'left', on = 'Season')
conf.loc[:,'game_date'] = conf.apply(lambda x: x.DayZero + pd.DateOffset(days = x.DayNum), axis = 1)
conf.drop(['Season','DayNum','DayZero'], axis = 1, inplace = True)

conf = pd.merge(conf, teams, how = 'left', left_on = 'WTeamID', right_on = 'TeamID')\
            .merge(teams, how = 'left', left_on = 'LTeamID', right_on = 'TeamID')\
            .drop(['TeamID_x','TeamID_y','WTeamID','LTeamID'], axis = 1)

conf.loc[:,'low_id'] = conf.apply(lambda x: min(x.ncaa_id_x,x.ncaa_id_y), axis = 1)
conf.loc[:,'high_id'] = conf.apply(lambda x: max(x.ncaa_id_x,x.ncaa_id_y), axis = 1)
conf.drop(['ncaa_id_x','ncaa_id_y'], axis = 1, inplace = True)

conf.loc[:,'conf'] = 1

with open('csv\\conf_tourn_games.csv', 'wb') as csvfile:
    conf.to_csv(csvfile, header = True, index = False)
