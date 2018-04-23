from __future__ import print_function
from functions.soupify import soupify
import pandas as pd
from collections import OrderedDict
import re
import os
import multiprocessing as mp
import sys


def data_scrape(game_id):
    
    url = 'http://stats.ncaa.org/game/period_stats/{0}'.format(game_id)
            
    soup = soupify(url)
    
    
    school_ids = soup.find('td', text = re.compile('total', re.I)).find_parent('table')\
        .find_all('tr', {'class': None})
    school_ids = [int(re.compile('(?<=team\/)[0-9]+(?=\/)').search(x.find('a').get('href')).group(0))
        if x.find('a') else None for x in school_ids]
    
    
    stats = soup.find('td', text = 'Game Stats').find_parent('table').find('table').find_all('tr', {'class': None})
    stats = [[y.text for y in x.find_all('td')] for x in stats]
    
    
    for i in [0,1]:
        
        data = OrderedDict()
                        
        data['game_id'] = [game_id]
        data['school_id'] = [school_ids[i]]
        
        for stat in stats:
            
            stat_name = '_'.join([x.lower()[:5] for x in stat[0].split(' ')])
            
            data[stat_name] = [stat[1 + i] if len(stat) >= 2 + i else None]
        
        summary = pd.concat([summary,pd.DataFrame(data)]) if 'summary' in locals().keys() else pd.DataFrame(data)
    
    
    return summary

def multi_proc(left, scraped, num_ids, file_loc, exist):
    
    finished = 0
        
    chunk_size = 5
    
    for section in range(0, len(left), chunk_size):
        
        percent_complete = '%5.2f'%(float(finished + scraped)/num_ids*100)
            
        sys.stdout.flush()
        print(' Game Summaries: {0}% Complete'.format(percent_complete, section), end = '\r')
        
        chunk = left[section : section + chunk_size]
        
        
        pool = mp.Pool(maxtasksperchild = 5)
        
        results = [pool.apply_async(data_scrape, args = (x)) for x in chunk]
        try:
            output = [p.get(timeout = 20) for p in results]
        except mp.TimeoutError:
            output = []
			
        for df in output:
            
            with open(file_loc, 'ab' if exist else 'wb') as csvfile:
                df.to_csv(csvfile, header = not exist, index = False)
                
            exist = True
        
        
        finished += len(output)


def single_proc(left, scraped, num_ids, file_loc, exist):
    
    finished = 0
    
    for game_id in left:
        
        percent_complete = '%5.2f'%(float(finished + scraped)/num_ids*100)
            
        sys.stdout.flush()
        print(' Game Summaries: {0}% Complete'.format(percent_complete), end = '\r')
            
        with open(file_loc, 'ab' if exist else 'wb') as csvfile:
            data_scrape(game_id).to_csv(csvfile, header = not exist, index = False)
            
        exist = True
            
        finished += 1


def update(game_ids_out, multi_proc_bool):
    
    print(' Game Summaries:', end = '\r')
    
    
    file_loc = 'csv\\game_summaries{0}.csv'.format('_multi' if multi_proc_bool else '_single')
    
    exist = os.path.isfile(file_loc)
    
    scraped = pd.read_csv(file_loc, header = 0, low_memory = False) if exist else pd.DataFrame(columns = ['game_id'])
    scraped = set(scraped.game_id)
    
    left = list(game_ids_out - scraped)
    scraped = len(scraped)
    
    if left:
        
        multi_proc(left, scraped, len(game_ids_out), file_loc, exist) if multi_proc_bool\
            else single_proc(left, scraped, len(game_ids_out), file_loc, exist)
    
    
    sys.stdout.flush()
    print(' Game Summaries: 100.00% Complete\n')