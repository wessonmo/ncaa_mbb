import parsers
import pandas as pd
from collections import OrderedDict
import re
import os
import multiprocessing as mp

#team scrapers
def team_indexes(seasons, divisions):
    file_loc = 'csv\\team_index.csv'
    file_exist = os.path.isfile(file_loc)
    indexes = pd.read_csv(file_loc) if file_exist else pd.DataFrame(columns = ['season','division'])
    miss_seasons = set((x,y) for x in seasons for y in divisions) - set(zip(indexes.season, indexes.division))
    
    if miss_seasons:
        pool = mp.Pool()
        results = [pool.apply_async(parsers.team_index, args = x) for x in miss_seasons]
        output = [p.get(timeout = 60) for p in results]
        
        for df in output:
            with open(file_loc, 'ab' if file_exist else 'wb') as csv_file:
                df.to_csv(csv_file, header = not file_exist, index = False)
            
            file_exist = True

        indexes = pd.read_csv(file_loc)[['season','school_id']]

    return indexes


def rosters(indexes):
    roster_loc = 'csv\\rosters.csv'
    roster_exist = os.path.isfile(roster_loc)
    rosters = set(tuple(x) for x in pd.read_csv(roster_loc)[['season_id','school_id']]) if roster_exist else set()
    miss_rosters = list(set(zip(indexes.season_id, indexes.school_id)) - rosters)
    
    if miss_rosters:
        chunk_size = 20

        for cutoff in range(0, len(miss_rosters), chunk_size):
            chunk = miss_rosters[cutoff : cutoff + chunk_size]

            pool = mp.Pool(maxtasksperchild = 5)
            results = [pool.apply_async(parsers.roster,args = x) for x in chunk]
            output = [p.get(timeout = 20) for p in results]                    
            
            for df in output:
                with open(roster_loc, 'ab' if roster_exist else 'wb') as csv_file:
                    df.to_csv(csv_file, header = not roster_exist, index = False)
                
                roster_exist = True


def team_info(indexes):
    for var_name in ['coaches','schedules','facilities']:
        file_loc = 'csv\\{0}.csv'.format(var_name)
        file_exist = os.path.isfile(file_loc)

        if file_exist:
            file_df = pd.read_csv(file_loc)[['season_id','school_id']].drop_duplicates()
            
            indexes = pd.merge(indexes, file_df, how = 'left', on = ['season_id','school_id'], indicator = var_name)
        else:
            indexes.loc[:,var_name] = 'left_only'
    
    miss_info = indexes.loc[indexes.apply(lambda x: 'left_only' in list(x), axis = 1)]
    
    if len(miss_info) > 0:
        chunk_size = 20

        for cutoff in range(0, len(miss_info), chunk_size):
            chunk = miss_info.iloc[cutoff : cutoff + chunk_size]
            
            pool = mp.Pool(maxtasksperchild = 5)
            results = [pool.apply_async(parsers.team_info, args = (row,)) for index, row in chunk.iterrows()]
            output = [p.get(timeout = 20) for p in results]
            
            for dict_ in output:
                for file_type in dict_.keys():
                    df = dict_[file_type]
                    
                    file_loc = 'csv\\{0}.csv'.format(file_type)
                    exist = os.path.isfile(file_loc)
                    
                    with open(file_loc, 'ab' if exist else 'wb') as csv_file:
                        df.to_csv(csv_file, header = not exist, index = False)

    return pd.read_csv('csv\\schedules.csv')[['game_id']].drop_duplicates()


#game scrapers
def game_summaries(game_ids):
    summary_loc = 'csv\\game_summaries.csv'
    summary_exist = os.path.isfile(summary_loc)
    summaries = set(pd.read_csv(summary_loc).game_id) if summary_exist else set()
    miss_summaries = list(set(game_ids.game_id) - summaries)
    
    if miss_summaries:
        chunk_size = 20

        for cutoff in range(0, len(miss_summaries), chunk_size):
            chunk = miss_summaries[cutoff : cutoff + chunk_size]

            pool = mp.Pool(maxtasksperchild = 5)
            results = [pool.apply_async(parsers.game_summary, args = x) for x in chunk]
            output = [p.get(timeout = 20) for p in results]                    
            
            for df in output:
                with open(summary_loc, 'ab' if summary_exist else 'wb') as csv_file:
                    df.to_csv(csv_file, header = not summary_exist, index = False)
                
                summary_exist = True


def box_scores(game_ids):
    box_loc = 'csv\\game_summaries.csv'
    box_exist = os.path.isfile(box_loc)
    box_scores = set(pd.read_csv(box_loc).game_id) if box_exist else set()
    miss_boxes = list(set(game_ids.game_id) - box_scores)
    
    if miss_boxes:
        chunk_size = 20

        for cutoff in range(0, len(miss_boxes), chunk_size):
            chunk = miss_boxes[cutoff : cutoff + chunk_size]

            pool = mp.Pool(maxtasksperchild = 5)
            results = [pool.apply_async(parsers.box_score, args = x) for x in chunk]
            output = [p.get(timeout = 20) for p in results]                    
            
            for df in output:
                with open(box_loc, 'ab' if box_exist else 'wb') as csv_file:
                    df.to_csv(csv_file, header = not box_exist, index = False)
                
                box_exist = True


def game_info(game_ids):
    for var_name in ['game_times','officials','pbps']:
        file_loc = 'csv\\{0}.csv'.format(var_name)
        file_exist = os.path.isfile(file_loc)

        if file_exist:
            file_df = pd.read_csv(file_loc)[['game_id']].drop_duplicates()
            
            game_ids = pd.merge(game_ids, file_df, how = 'left', on = ['game_id'], indicator = var_name)
        else:
            game_ids.loc[:,var_name] = 'left_only'
    
    miss_info = game_ids.loc[indexes.apply(lambda x: 'left_only' in list(x), axis = 1)]
    
    if len(miss_info) > 0:
        chunk_size = 20

        for cutoff in range(0, len(miss_info), chunk_size):
            chunk = miss_info.iloc[cutoff : cutoff + chunk_size]
            
            pool = mp.Pool(maxtasksperchild = 5)
            results = [pool.apply_async(parsers.game_info, args = (row,)) for index, row in chunk.iterrows()]
            output = [p.get(timeout = 20) for p in results]
            
            for dict_ in output:
                for file_type in dict_.keys():
                    df = dict_[file_type]
                    
                    file_loc = 'csv\\{0}.csv'.format(file_type)
                    exist = os.path.isfile(file_loc)
                    
                    with open(file_loc, 'ab' if exist else 'wb') as csv_file:
                        df.to_csv(csv_file, header = not exist, index = False)