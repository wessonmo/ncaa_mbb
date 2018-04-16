from __future__ import print_function
#from sys import argv
#import multiprocessing as mp
from scrapers import *
#import time
#import sys
#import pandas as pd
#from __future__ import division
from sys import argv
#import tqdm



if __name__ == '__main__':
    
    seasons = range(int(argv[1]), int(argv[2]) + 1) #range(2016,2019)
    divisions = range(int(argv[3]), int(argv[4]) + 1) #[1]
    
    index = school_index.update(seasons, divisions)
    
    rosters.update(index)
    facilities_coaches_and_schedules.update(index)
    game_ids = game_ids.update(index)
    
    box_scores.update(game_ids)
    times_officials_and_pbps.update(game_ids)
    game_summaries.update(game_ids)