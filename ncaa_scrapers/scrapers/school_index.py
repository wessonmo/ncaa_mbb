from __future__ import print_function
from functions.soupify import soupify
from collections import OrderedDict
import re
import os.path
import pandas as pd
import sys
import multiprocessing as mp


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
    
    data = pd.DataFrame(data)
    
    if len(data) > 0: return data
    else: raise Exception('No data: {0}'.format(url))


def multi_proc(left, file_loc, exist):
    
    pool = mp.Pool()
        
    results = [pool.apply_async(data_scrape, args = x) for x in left]
    output = [p.get(timeout = 60) for p in results]
    
    for df in output:
        
        with open(file_loc, 'ab' if exist else 'wb') as csvfile:
            df.to_csv(csvfile, header = not exist, index = False)
            
        exist = True
        
    return pd.read_csv(file_loc, header = 0)


def single_proc(left, file_loc, exist):
    
    for season, division in left:
        
        with open(file_loc, 'ab' if exist else 'wb') as csvfile:
            data_scrape(season, division).to_csv(csvfile, header = not exist, index = False)
                
            exist = True
    
    return pd.read_csv(file_loc, header = 0)


def update(seasons, divisions, multi_proc_bool):
    
    print(' School IDs:', end = '\r')
    
    
    file_loc = 'csv\\school_index{0}.csv'.format('_multi' if multi_proc_bool else '_single')
    
    exist = os.path.isfile(file_loc)
        
    index = pd.read_csv(file_loc, header = 0) if exist else pd.DataFrame(columns = ['season', 'division'])
    
    
    scraped = set(zip(index.season, index.division))
            
    needed = set((x,y) for x in seasons for y in divisions)
    
    left = needed - scraped
    
    
    if left:
        
        percent_complete = '%5.2f'%(float(len(scraped))/len(needed)*100)
            
        sys.stdout.flush()
        print(' School IDs: {0}% Complete'.format(percent_complete), end = '\r')
        
        index = multi_proc(left, file_loc, exist) if multi_proc_bool else single_proc(left, file_loc, exist)
        
    sys.stdout.flush()
    print(' School IDs: 100.00% Complete\n')
    return index