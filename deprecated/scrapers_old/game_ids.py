from __future__ import print_function
from functions.soupify import soupify
import pandas as pd
from collections import OrderedDict
import re
import os
import multiprocessing as mp
import sys


def data_scrape(season_id, school_id):
    
    url = 'http://stats.ncaa.org/team/{0}/{1}'.format(school_id, season_id)
        
    soup = soupify(url)
    
    
    if soup.find('td', text = re.compile('Schedule\/Results')) == None:
        
        data = pd.DataFrame([[season_id, school_id]], columns = ['season_id','school_id'])
        
    else:
        
        games = [x for x in soup.find('td', text = re.compile('Schedule\/Results')).find_parent('table')\
            .find_all('tr', {'class': None}) if len(x.find_all('td')) > 1]
        
        data = OrderedDict()
            
        data['season_id'] = [season_id]*len(games)
        data['school_id'] = [school_id]*len(games)
        data['game_date'] = [x.find('td').text for x in games]
        data['opp_id'] = [int(x.find_all('td')[1].find('a').get('href').split('/')[2])
            if x.find_all('td')[1].find('a') != None else None for x in games]
        data['game_id'] = [int(x.find_all('td')[2].find('a').get('href').split('?')[0].split('/')[-1])
            if x.find_all('td')[2].find('a') != None else None for x in games]
        
        data = pd.DataFrame(data)
        data.loc[:,'game_date'] = pd.to_datetime(data.loc[:,'game_date'])
        
    return data


def multi_proc(left, scraped, needed, file_loc, exist):
    
    finished = 0
        
    chunk_size = 20        
    
    for section in range(0, len(left), chunk_size):
        
        percent_complete = '%5.2f'%(float(finished + scraped)/needed*100)
            
        sys.stdout.flush()
        print(' Game IDs: {0}% Complete'.format(percent_complete, section), end = '\r')
        
        chunk = left[section : section + chunk_size]
        
        
        pool = mp.Pool(maxtasksperchild = 5)
        
        results = [pool.apply_async(data_scrape, args = x) for x in chunk]
        try:
            output = [p.get(timeout = 20) for p in results]
        except mp.TimeoutError:
            output = []
        
        
        for df in output:
            
            with open(file_loc, 'ab' if exist else 'wb') as csvfile:
                df.to_csv(csvfile, header = not exist, index = False)
                
            exist = True
        
            
        finished += len(output)
        
    return pd.read_csv(file_loc, header = 0)


def single_proc(left, scraped, needed, file_loc, exist):
    
    finished = 0
    
    for season_id, school_id in left:
        
        percent_complete = '%5.2f'%(float(finished + scraped)/needed*100)
            
        sys.stdout.flush()
        print(' Game IDs: {0}% Complete'.format(percent_complete), end = '\r')
        
        with open(file_loc, 'ab' if exist else 'wb') as csvfile:
            data_scrape(season_id, school_id).to_csv(csvfile, header = not exist, index = False)
                
        exist = True
        
        finished += 1
        
    return pd.read_csv(file_loc, header = 0)


def update(index, multi_proc_bool):
    
    print(' Game IDs:', end = '\r')
    
    
    file_loc = 'csv\\game_ids{0}.csv'.format('_multi' if multi_proc_bool else '_single')
    
    exist = os.path.isfile(file_loc)
    
    game_ids = pd.read_csv(file_loc, header = 0) if exist else pd.DataFrame(columns = ['season_id', 'school_id'])
    
    
    scraped = set(zip(game_ids.season_id, game_ids.school_id))
    
    needed = set(zip(index.season_id, index.school_id))
    
    left = needed - scraped
    
    scraped, needed = len(scraped), len(needed)
    
    
    if left:
        
        game_ids = multi_proc(left, scraped, needed, file_loc, exist) if multi_proc_bool\
            else single_proc(left, scraped, needed, file_loc, exist)
            
    skip_ids = set([4274098,4042151])
            
    sys.stdout.flush()
    print(' Game IDs: 100.00% Complete\n')
    
    return set(game_ids.loc[~pd.isnull(game_ids.game_id)].game_id) - skip_ids