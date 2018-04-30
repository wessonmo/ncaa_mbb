from __future__ import print_function
from scrapers import *
from sys import argv
import time
import pandas as pd


if __name__ == '__main__':
    
    seasons = range(int(argv[1]), int(argv[2]) + 1) #range(2016,2019)
    divisions = range(int(argv[3]), int(argv[4]) + 1) #[1]
    
    multi_proc_bool = True if argv[5] == 1 else False
    
    start = time.time()
    
    index = school_index.update(seasons, divisions, multi_proc_bool)
    
    print('{0} seconds'.format(time.time() - start))
    start = time.time()
    
    rosters.update(index, multi_proc_bool)
    
    print('{0} seconds'.format(time.time() - start))
    start = time.time()
    
    facilities_coaches_and_schedules.update(index, multi_proc_bool)
    
    print('{0} seconds'.format(time.time() - start))
    start = time.time()
    
    game_ids_out = game_ids.update(index, multi_proc_bool)
    
    print('{0} seconds'.format(time.time() - start))
    start = time.time()
    
    box_scores.update(game_ids_out, multi_proc_bool)
    
    print('{0} seconds'.format(time.time() - start))
    start = time.time()
    
    times_officials_and_pbps.update(game_ids_out, multi_proc_bool)
    
    print('{0} seconds'.format(time.time() - start))
    start = time.time()
    
    game_summaries.update(game_ids_out, multi_proc_bool)
    
    print('{0} seconds'.format(time.time() - start))