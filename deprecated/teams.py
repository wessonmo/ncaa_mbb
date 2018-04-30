from __future__ import print_function
from functions.soupify import soupify
import scrapers
import pandas as pd
from collections import OrderedDict
import re
import os
import multiprocessing as mp
import sys


def init_team_index(seasons, divisions = [1]):
    #Scrape missing team indexes, create index dataframe
    file_loc = 'csv\\team_index.csv'
    file_exist = os.path.isfile(file_loc)
    index = pd.read_csv(file_loc) if file_exist else pd.DataFrame(columns = ['season','division'])
    miss_seasons = set((x,y) for x in seasons for y in divisions) - set(zip(index.season, index.division))
    
    if miss_seasons:
        pool = mp.Pool()
        results = [pool.apply_async(scrapers.team_index, args = x) for x in miss_seasons]
        output = [p.get(timeout = 60) for p in results]
        
        for df in output:
            with open(file_loc, 'ab' if file_exist else 'wb') as csv_file:
                df.to_csv(csv_file, header = not file_exist, index = False)
            
            file_exist = True

        index = pd.read_csv(file_loc)[['season','school_id']]

    return index

        
def scrape_rosters(index):
    #Scrape missing rosters
    roster_loc = 'csv\\rosters.csv'
    roster_exist = os.path.isfile(roster_loc)
    rosters = set(tuple(x) for x in pd.read_csv(roster_loc)[['season_id','school_id']]) if roster_exist else set()
    miss_rosters = list(set(zip(index.season_id, index.school_id)) - rosters)
    
    if miss_rosters:
        chunk_size = 20

        for cutoff in range(0, len(miss_rosters), chunk_size):
            chunk = miss_rosters[cutoff : cutoff + chunk_size]

            pool = mp.Pool(maxtasksperchild = 5)
            results = [pool.apply_async(scrapers.rosters,args = x) for x in chunk]
            output = [p.get(timeout = 20) for p in results]                    
            
            for df in output:
                with open(roster_loc, 'ab' if roster_exist else 'wb') as csv_file:
                    df.to_csv(csv_file, header = not roster_exist, index = False)
                
                roster_exist = True

    
def scrape_team_info(index, game_queue):
    #Scrape missing team_info
    for var_name in ['coaches','schedules','facilities']:
        file_loc = 'csv\\{0}.csv'.format(var_name)
        file_exist = os.path.isfile(file_loc)

        if file_exist:
            file_df = pd.read_csv(file_loc)
            if var_name == 'schedules': map(game_queue.put, list(file_df.game_id.unique()) + ['stop'])
            file_df = file_df[['season_id','school_id']].drop_duplicates()
            
            index = pd.merge(index, file_df, how = 'left', on = ['season_id','school_id'], indicator = var_name)
        else:
            index.loc[:,var_name] = 'left_only'
    
    miss_info = index.loc[index.apply(lambda x: 'left_only' in list(x), axis = 1)]
    
    if len(miss_info) > 0:
        chunk_size = 20

        for cutoff in range(0, len(miss_info), chunk_size):
            chunk = miss_info.iloc[cutoff : cutoff + chunk_size]
            
            pool = mp.Pool(maxtasksperchild = 5)
            results = [pool.apply_async(scrapers.team_info, args = (row,)) for index, row in chunk.iterrows()]
            output = [p.get(timeout = 20) for p in results]
            
            for dict_ in output:
                for file_type in dict_.keys():
                    df = dict_[file_type]
                    if file_type == 'schedules':
                        exist_games = [i for i in iter(game_queue, 'stop')]
                        map(game_queue.put, [x for x in df.game_id.unique() if x not in exist_games])
                    
                    file_loc = 'csv\\{0}.csv'.format(file_type)
                    exist = os.path.isfile(file_loc)
                    
                    with open(file_loc, 'ab' if exist else 'wb') as csv_file:
                        df.to_csv(csv_file, header = not exist, index = False)

    game_queue.put('exit')