from __future__ import print_function
from functions.soupify import soupify
import pandas as pd
from collections import OrderedDict
import re
import os
import multiprocessing as mp
import sys


def data_scrape(game_id):
    
    if 'summary' in set(globals().keys()) & set(locals().keys()):
        del summary
    
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
        
        summary = pd.concat([summary,pd.DataFrame(data)]) if 'summary' in set(globals().keys()) | set(locals().keys())\
            else pd.DataFrame(data)
    
    
    return summary


def update(game_ids):
    
    print(' Game Summaries:', end = '\r')
    
    
    file_loc = 'csv\\game_summaries.csv'
    
    exist = os.path.isfile(file_loc)
    
    summaries = pd.read_csv(file_loc, header = 0) if exist else pd.DataFrame(columns = ['game_id'])
    
    
    scraped = set(summaries.game_id)
    
    needed = game_ids
    
    left = list(needed - scraped)
    
    
    if left:
        
        finished = 0
        
        chunk_size = 20
        
        for section in range(0, len(left), chunk_size):
            
            percent_complete = '%5.2f'%(float(finished + len(scraped))/len(needed)*100)
                
            sys.stdout.flush()
            print(' Game Summaries: {0}% Complete'.format(percent_complete, section), end = '\r')
            
            chunk = left[section : section + chunk_size]
            
            
            pool = mp.Pool()
            
            results = [pool.apply_async(data_scrape, args = (x)) for x in chunk]
            output = [p.get() for p in results]
            
            
            for df in output:
                
                with open(file_loc, 'ab' if exist else 'wb') as csvfile:
                    df.to_csv(csvfile, header = not exist, index = False)
                    
                exist = True
            
            
            finished += chunk_size            
            
    
    sys.stdout.flush()
    print(' Game Summaries: 100.00% Complete\n')