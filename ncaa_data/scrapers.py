from __future__ import print_function
import sys
import parsers
import pandas as pd
from collections import OrderedDict
import re
import os
import multiprocessing as mp

#team scrapers
def team_indexes(seasons, divisions):
    print(' {0: >14}:'.format('Team Indexes'), end = '\r')
    
    file_loc = 'csv\\team_index.csv'
    file_exist = os.path.isfile(file_loc)
    indexes = pd.read_csv(file_loc) if file_exist else pd.DataFrame(columns = ['season','division'])
    miss_seasons = set((x,y) for x in seasons for y in divisions) - set(zip(indexes.season, indexes.division))

    completed, needed = len(set(zip(indexes.season, indexes.division))), len(set((x,y) for x in seasons for y in divisions))
    sys.stdout.flush()
    print(' {0: >14}: {1: >7}/{2: <7}'.format('Team Indexes',completed, needed), end = '\r')

    for season, division in miss_seasons:
        output = parsers.team_index(season, division)

        with open(file_loc, 'ab' if file_exist else 'wb') as csv_file:
            output.to_csv(csv_file, header = not file_exist, index = False)
        file_exist = True

        completed += 1
        sys.stdout.flush()
        print(' {0: >14}: {1: >7}/{2: <7}'.format('Team Indexes',completed, needed), end = '\r')

    sys.stdout.flush()
    print(' {0: >14}: {1: >7}/{2: <7}'.format('Team Indexes',completed, needed))


def rosters():
    print(' {0: >14}:'.format('Rosters'), end = '\r')

    indexes = pd.read_csv('csv\\team_index.csv')[['season_id','school_id']]

    roster_loc = 'csv\\rosters.csv'
    roster_exist = os.path.isfile(roster_loc)
    rosters = set((row.season_id, row.school_id) for index, row in pd.read_csv(roster_loc)[['season_id','school_id']].iterrows())\
        if roster_exist else set()
    miss_rosters = list(set(zip(indexes.season_id, indexes.school_id)) - rosters)

    completed, needed = len(rosters), len(set(zip(indexes.season_id, indexes.school_id)))
    sys.stdout.flush()
    print(' {0: >14}: {1: >7}/{2: <7}'.format('Rosters',completed, needed), end = '\r')
    
    for season_id, school_id in miss_rosters:
        output = parsers.roster(season_id, school_id)

        with open(roster_loc, 'ab' if roster_exist else 'wb') as csv_file:
            output.to_csv(csv_file, header = not roster_exist, index = False)
        roster_exist = True

        completed += 1
        sys.stdout.flush()
        print(' {0: >14}: {1: >7}/{2: <7}'.format('Rosters',completed, needed), end = '\r')

    sys.stdout.flush()
    print(' {0: >14}: {1: >7}/{2: <7}'.format('Rosters',completed, needed))


def team_info():
    print(' {0: >14}:'.format('Team Info'), end = '\r')

    indexes = pd.read_csv('csv\\team_index.csv')[['season','season_id','school_id']]

    for var_name in ['coaches','schedules','facilities']:
        file_loc = 'csv\\{0}.csv'.format(var_name)
        file_exist = os.path.isfile(file_loc)

        if file_exist:
            file_df = pd.read_csv(file_loc)[['season_id','school_id']].drop_duplicates()
            
            indexes = pd.merge(indexes, file_df, how = 'left', on = ['season_id','school_id'], indicator = var_name)
        else:
            indexes.loc[:,var_name] = 'left_only'
    
    miss_info = indexes.loc[indexes.apply(lambda x: 'left_only' in list(x), axis = 1)]

    completed, needed = len(indexes) - len(miss_info), len(indexes)
    sys.stdout.flush()
    print(' {0: >14}: {1: >7}/{2: <7}'.format('Team Info',completed, needed), end = '\r')

    for index, row in miss_info.iterrows():
        dict_ = parsers.team_info(row)

        for file_type in dict_.keys():
            df = dict_[file_type]
            
            file_loc = 'csv\\{0}.csv'.format(file_type)
            exist = os.path.isfile(file_loc)
            
            with open(file_loc, 'ab' if exist else 'wb') as csv_file:
                df.to_csv(csv_file, header = not exist, index = False)

        completed += 1
        sys.stdout.flush()
        print(' {0: >14}: {1: >7}/{2: <7}'.format('Team Info',completed, needed), end = '\r')

    sys.stdout.flush()
    print(' {0: >14}: {1: >7}/{2: <7}'.format('Team Info',completed, needed))


#game scrapers
def game_summaries():
    print(' {0: >14}:'.format('Game Summaries'), end = '\r')

    game_ids = set(row.game_id for index, row in pd.read_csv('csv\\schedules.csv').iterrows() if not pd.isnull(row.game_id))

    summary_loc = 'csv\\game_summaries.csv'
    summary_exist = os.path.isfile(summary_loc)
    summaries = set(pd.read_csv(summary_loc).game_id) if summary_exist else set()
    miss_summaries = list(game_ids - summaries)

    completed, needed = len(summaries), len(game_ids)
    sys.stdout.flush()
    print(' {0: >14}: {1: >7}/{2: <7}'.format('Game Summaries',completed, needed), end = '\r')

    for game_id in miss_summaries:
        output = parsers.game_summary(game_id)

        with open(summary_loc, 'ab' if summary_exist else 'wb') as csv_file:
            output.to_csv(csv_file, header = not summary_exist, index = False)
        summary_exist = True

        completed += 1
        sys.stdout.flush()
        print(' {0: >14}: {1: >7}/{2: <7}'.format('Game Summaries',completed, needed), end = '\r')

    sys.stdout.flush()
    print(' {0: >14}: {1: >7}/{2: <7}'.format('Game Summaries',completed, needed))


def box_scores():
    print(' {0: >14}:'.format('Box Scores'), end = '\r')

    game_ids = set(row.game_id for index, row in pd.read_csv('csv\\schedules.csv').iterrows() if not pd.isnull(row.game_id))

    box_loc = 'csv\\box_scores.csv'
    box_exist = os.path.isfile(box_loc)
    box_scores = set(pd.read_csv(box_loc).game_id) if box_exist else set()
    miss_boxes = list(game_ids - box_scores)

    completed, needed = len(box_scores), len(game_ids)
    sys.stdout.flush()
    print(' {0: >14}: {1: >7}/{2: <7}'.format('Box Scores',completed, needed), end = '\r')

    for game_id in miss_boxes:
        output = parsers.box_score(game_id)

        if len(output) == 0:
            continue

        with open(box_loc, 'ab' if box_exist else 'wb') as csv_file:
            output.to_csv(csv_file, header = not box_exist, index = False)
        box_exist = True

        completed += 1
        sys.stdout.flush()
        print(' {0: >14}: {1: >7}/{2: <7}'.format('Box Scores',completed, needed), end = '\r')

    sys.stdout.flush()
    print(' {0: >14}: {1: >7}/{2: <7}'.format('Box Scores',completed, needed))


def game_info():
    print(' {0: >14}:'.format('Game Info'), end = '\r')

    game_ids = pd.read_csv('csv\\schedules.csv')[['game_id']].drop_duplicates()
    game_ids = game_ids.loc[~pd.isnull(game_ids.game_id),:]

    for var_name in ['game_times','game_locs','officials','pbps']:
        file_loc = 'csv\\{0}.csv'.format(var_name)
        file_exist = os.path.isfile(file_loc)

        if file_exist:
            file_df = pd.read_csv(file_loc)[['game_id']].drop_duplicates()
            
            game_ids = pd.merge(game_ids, file_df, how = 'left', on = ['game_id'], indicator = var_name)
        else:
            game_ids.loc[:,var_name] = 'left_only'
    
    miss_info = game_ids.loc[game_ids.apply(lambda x: 'left_only' in list(x), axis = 1)]

    completed, needed = len(game_ids) - len(miss_info), len(game_ids)
    sys.stdout.flush()
    print(' {0: >14}: {1: >7}/{2: <7}'.format('Game Info',completed, needed), end = '\r')

    for index, row in miss_info.iterrows():
        dict_ = parsers.game_info(row)

        for file_type in dict_.keys():
            df = dict_[file_type]
            
            file_loc = 'csv\\{0}.csv'.format(file_type)
            exist = os.path.isfile(file_loc)
            
            with open(file_loc, 'ab' if exist else 'wb') as csv_file:
                df.to_csv(csv_file, header = not exist, index = False)

        completed += 1
        sys.stdout.flush()
        print(' {0: >14}: {1: >7}/{2: <7}'.format('Game Info',completed, needed), end = '\r')

    sys.stdout.flush()
    print(' {0: >14}: {1: >7}/{2: <7}'.format('Game Info',completed, needed))

if __name__ == '__main__':
    pass