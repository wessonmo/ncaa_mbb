from __future__ import print_function
#from sys import argv
import multiprocessing as mp
from scrapers import *
import time
import sys
import pandas as pd
from __future__ import division
import sys



if __name__ == '__main__':
    
    seasons = range(2016,2019)#range(int(argv[1]), int(argv[2]) + 1)
    divisions = [1]#range(int(argv[3]), int(argv[4]) + 1)
    
    index_loc = 'csv\\school_index.csv'
    
    missing_index = school_index.data_check(index_loc, seasons, divisions)
    
    if missing_index:
        
        pool = mp.Pool()
        
        finished = []
        
        results = [pool.apply_async(school_index.scrape, args=(x), callback = finished.append) for x in missing_index]
        
        while len(finished) < len(missing_index):
            
            print('{0}%'.format(float(len(finished))/len(missing_index)))
            
            time.sleep(0.5)
        
        output = [pd.DataFrame(p.get()).head() for p in results]