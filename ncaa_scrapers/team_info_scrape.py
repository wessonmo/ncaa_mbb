from __future__ import print_function
from functions.soupify import soupify
import pandas as pd
from collections import OrderedDict
import re
import os
import multiprocessing as mp
import sys

class team_info_scrape:
    
    def __init__(self, seasons, divisions):
        self.seasons = seasons
        self.divisions = divisions
    
    @classmethod
    def check_indexes(cls):
        file_loc = 'csv\\school_index.csv'
        
        needed = set((x,y) for x in cls.seasons for y in cls.divisions)
        
        if os.path.isfile(file_loc):
            index = pd.read_csv(file_loc)[['season','division']].drop_duplicates()
            cls.index_exist = True
            cls.indexes_needed = needed - set(zip(index.season, index.division))
        else:
            cls.index_exist = False
            cls.indexes_needed = needed
            
    @classmethod
    def scrape_indexes(cls):
        file_loc = 'csv\\school_index.csv'
        
        pool = mp.Pool()
        
        results = [pool.apply_async(data_scrape, args = x) for x in cls.indexes_needed]
        output = [p.get(timeout = 60) for p in results]
        
        for df in output:
            
            with open(file_loc, 'ab' if cls.index_exist else 'wb') as csvfile:
                df.to_csv(csvfile, header = not cls.index_exist, index = False)
            
            cls.index_exist = True
            
            
    
        
if __name__ == '__main__':
    
    team_info_scrape.seasons = range(2012, 2019)
    team_info_scrape.divisions = [1]
    
    team_info_scrape.check_indexes()
    if len(team_info_scrape.indexes_needed) > 0:
        team_info_scrape.scrape_indexes()