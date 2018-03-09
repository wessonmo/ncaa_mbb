import pandas as pd

teams = pd.read_csv('kaggle\\data\\Teams.csv', header = 0)[['TeamID','ncaa_id']]

seasons = pd.read_csv('kaggle\\data\\Seasons.csv', header = 0)[['Season','DayZero']]
seasons.loc[:,'DayZero'] = pd.to_datetime(seasons.DayZero)

tourney = pd.read_csv('kaggle\\data\\NCAATourneyCompactResults.csv', header = 0)[['Season','DayNum','WTeamID','LTeamID']]

tourney = pd.merge(tourney, seasons, how = 'left', on = 'Season')
tourney.loc[:,'game_date'] = tourney.apply(lambda x: x.DayZero + pd.DateOffset(days = x.DayNum), axis = 1)
tourney.drop(['Season','DayNum','DayZero'], axis = 1, inplace = True)

tourney = pd.merge(tourney, teams, how = 'left', left_on = 'WTeamID', right_on = 'TeamID')\
            .merge(teams, how = 'left', left_on = 'LTeamID', right_on = 'TeamID')\
            .drop(['TeamID_x','TeamID_y','WTeamID','LTeamID'], axis = 1)

tourney.loc[:,'school_id'] = tourney.apply(lambda x: min(x.ncaa_id_x,x.ncaa_id_y), axis = 1)
tourney.loc[:,'opp_id'] = tourney.apply(lambda x: max(x.ncaa_id_x,x.ncaa_id_y), axis = 1)
tourney.drop(['ncaa_id_x','ncaa_id_y'], axis = 1, inplace = True)

tourney.loc[:,'tourn'] = 1

with open('csv\\tourn_games.csv', 'wb') as csvfile:
    tourney.to_csv(csvfile, header = True, index = False)