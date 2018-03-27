import pandas as pd
from itertools import combinations
from collections import OrderedDict

seasons = pd.read_csv('kaggle\\csv\\Seasons.csv', header = 0).drop(['RegionW','RegionX','RegionY','RegionZ'], axis = 1)
seasons.loc[:,'DayZero'] = pd.to_datetime(seasons.DayZero)

sites = pd.read_csv('kaggle\\csv\\ncaa_sites.csv', header = 0)

seeds = pd.read_csv('kaggle\\csv\\NCAATourneySeeds.csv', header = 0)
season = seeds.Season.max()
    
slots = pd.read_csv('kaggle\\csv\\NCAATourneySeedRoundSlots.csv', header = 0)
slots = pd.merge(slots, seeds.loc[seeds.Season == season], how = 'left', on = 'Seed')\
        .merge(seasons, how = 'left', on = 'Season')\
        .merge(sites, how = 'left', on = ['Season','GameSlot'])
slots = slots.loc[pd.isnull(slots.TeamID) == False]
slots.loc[:,'early_date'] = slots.apply(lambda x: x.DayZero + pd.DateOffset(days = x.EarlyDayNum), axis = 1)
slots.loc[:,'late_date'] = slots.apply(lambda x: x.DayZero + pd.DateOffset(days = x.LateDayNum), axis = 1)
slots.drop(['EarlyDayNum','LateDayNum','DayZero','Season'], axis = 1, inplace = True)

possible_games = pd.DataFrame(columns = ['round','site','early_date','late_date','low_kaggle','high_kaggle'])
old_comb = set()

for rnd in range(1,7):
    gameslots  = set(slots.loc[slots.GameRound == rnd,'GameSlot'])
    
    for gameslot in gameslots:
        id_set = set(slots.loc[(slots.GameSlot == gameslot) & (pd.isnull(slots.TeamID) == False),'TeamID'])
        new_comb = set(tuple(sorted(x)) for x in combinations(id_set, 2)) - old_comb
        
        old_comb = old_comb | new_comb
        
        data = OrderedDict()
        
        data['round'] = [slots.loc[slots.GameSlot == gameslot,'GameRound'].iloc[0]]*len(new_comb)
        data['site'] = [slots.loc[slots.GameSlot == gameslot,'Site'].iloc[0]]*len(new_comb)
        data['early_date'] = [slots.loc[slots.GameSlot == gameslot,'early_date'].iloc[0]]*len(new_comb)
        data['late_date'] = [slots.loc[slots.GameSlot == gameslot,'late_date'].iloc[0]]*len(new_comb)
        data['low_kaggle'] = [min(x) for x in new_comb]
        data['high_kaggle'] = [max(x) for x in new_comb]
        
        new_games = pd.DataFrame(data)
        
        possible_games = possible_games.append(pd.DataFrame(data))