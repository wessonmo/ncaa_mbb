from __future__ import print_function
from soupify import soupify
from collections import OrderedDict
import re
import os.path
import pandas as pd
import sys
import multiprocessing as mp
import time


def data_scrape(season, division):
        
    url = 'http://stats.ncaa.org/team/inst_team_list?academic_year={0}&conf_id=-1&division={1}&sport_code=MBB'\
        .format(season, division)
    
    soup = soupify(url)
    
    schools = soup.find_all('a', href = re.compile('\/team\/[0-9]+\/[0-9]+$'))
    
    data = OrderedDict()
    
    data['season'] = [season]*len(schools)
    data['season_id'] = [int(x.get('href').split('/')[-1]) for x in schools]
    data['school_id'] = [int(x.get('href').split('/')[2]) for x in schools]
    data['school_name'] = [x.text for x in schools]
    data['division'] = [division]*len(schools)
    
    return pd.DataFrame(data)


def update(seasons, divisions):
    
    print(' School IDs:', end = '\r')
    
    
    file_loc = 'csv\\school_index.csv'
    
    exist = os.path.isfile(file_loc)
        
    index = pd.read_csv(file_loc, header = 0) if exist else pd.DataFrame(columns = ['season', 'division'])
    
    
    scraped = set(zip(index.season, index.division))
            
    needed = set((x,y) for x in seasons for y in divisions)
    
    left = needed - scraped
    
    
    if left:
        
        finished = []
        
        
        pool = mp.Pool(processes = mp.cpu_count() - 1)
        
        results = [pool.apply_async(data_scrape, args = x, callback = finished.append) for x in left]
        
        
        while len(finished) < len(left):
            
            percent_complete = '%5.2f'%(float(len(finished) + len(scraped))/len(needed)*100)
            
            sys.stdout.flush()
            print(' School IDs: {0}% Complete'.format(percent_complete), end = '\r')
            
            time.sleep(0.5)
            
        
        output = [p.get() for p in results]
        
        for df in output:
            
            with open(file_loc, 'ab' if exist else 'wb') as csvfile:
                df.to_csv(csvfile, header = not exist, index = False)
                
            exist = True
            
            
        index = pd.read_csv(file_loc, header = 0)
        
    sys.stdout.flush()
    print(' School IDs: 100.00% Complete\n')
    return index