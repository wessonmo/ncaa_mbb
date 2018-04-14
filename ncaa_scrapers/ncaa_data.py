from __future__ import print_function
from sys import argv
from multiprocessing import Process
from scrapers import *
import time
import sys
import re

if __name__ == '__main__':
    seasons = range(int(argv[1]), int(argv[2]) + 1)
    divisions = range(int(argv[3]), int(argv[4]) + 1)
    
    print('Scraping: School Index', end = '\r')
    
    school_index.scraper(seasons, divisions)
    
    sys.stdout.flush()
    print('Complete: School Index\n')
    
    
    print('Scraping: Facilities, Coaches, Schedules, Rosters, and Game IDs', end = '\r')
    start = time.time()
    p2a = Process(target = facilities_coaches_and_schedules.scraper)
    p2a.start()
    
    p2b = Process(target = rosters.scraper)
    p2b.start()

    p2c = Process(target = game_ids.scraper)
    p2c.start()    
    
    p2a.join()
    p2b.join()
    p2c.join()
    
    sys.stdout.flush()
    print('Complete: Facilities, Coaches, Schedules, Rosters, and Game IDs\n')
    
    
    print('Scraping: Box Scores, Game Times, Officials, and Play-by-plays', end = '\r')
    
    p3a = Process(target = box_scores.scraper)
    p3a.start()
    
    p3b = Process(target = times_officials_and_pbps.scraper)
    p3b.start()
    
    p3a.join()
    p3b.join()
    end = time.time() - start
    sys.stdout.flush()
    print('Complete: Box Scores, Game Times, Officials, and Play-by-plays\n')
    print(end)