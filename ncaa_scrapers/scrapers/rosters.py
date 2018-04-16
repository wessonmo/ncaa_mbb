from __future__ import print_function
from functions.soupify import soupify
import pandas as pd
from collections import OrderedDict
import re
import os
import multiprocessing as mp
import sys
import time


def data_scrape(season_id, school_id):
    
    url = 'http://stats.ncaa.org/team/{0}/roster/{1}'.format(school_id, season_id)
        
    soup = soupify(url)
    
    players = soup.find('th', text = re.compile('Roster')).find_parent('table').find('tbody').find_all('tr')
    players = [[y.contents[0] if y.contents else '' for y in x.find_all('td')] for x in players]
    
    data = OrderedDict()
        
    data['season_id'] = [season_id]*len(players)
    data['school_id'] = [school_id]*len(players)
    data['jersey'] = [int(x[0]) if re.compile('^[0-9]{1,2}$').search(x[0].strip()) else None for x in players]
    data['player_id'] = [int(x[1].get('href').split('=')[-1]) if x[1].name == 'a' else None for x in players]
    data['player_name'] = [re.sub(r'[^\x00-\x7F]+','',x[1].text) if x[1].name == 'a'
        else re.sub(r'[^\x00-\x7F]+','',x[1]) for x in players]
    data['pos'] = [x[2] if x[2] != '' else None for x in players]
    data['height'] = [x[3] if x[3] not in ['','-'] else None for x in players]
    data['class'] = [x[4] if x[4] not in ['','N/A'] else None for x in players]
    
    data = pd.DataFrame(data)
    data = data.loc[data.jersey.isin(range(100) + [None]) & (data.player_name != 'Use, Don\'t')]
    
    return data
    
    
def update(index):
    
    print(' Rosters:', end = '\r')
    
    
    file_loc = 'csv\\rosters.csv'
    
    exist = os.path.isfile(file_loc)
    
    rosters = pd.read_csv(file_loc, header = 0) if exist else pd.DataFrame(columns = ['season_id', 'school_id'])
    
    
    scraped = set(zip(rosters.season_id, rosters.school_id))
    
    needed = set(zip(index.season_id, index.school_id))
    
    left = list(needed - scraped)
    
    
    if left:
        
        finished = []
        
        
        for section in range(0, len(left), 10):
            
            chunk = left[section : section + 10]
            
            
            chunk_finished = []
            
            pool = mp.Pool(processes = mp.cpu_count() - 1)
            
            results = [pool.apply_async(data_scrape, args = x, callback = chunk_finished.append) for x in chunk]
        
            start = time.time()
            while len(chunk_finished) < len(chunk):
                
                if time.time() - start >= 600:
                    
                    for p in results: p.terminate()
                    
                    raise ValueError('chunk took too long')
                    
                percent_complete = '%5.2f'%(float(len(chunk_finished) + len(finished) + len(scraped))/len(needed)*100)
                
                sys.stdout.flush()
                print(' Rosters: {0}% Complete'.format(percent_complete), end = '\r')
                
                time.sleep(0.5)
                
            
            output = [p.get() for p in results]
            
            for df in output:
                
                with open(file_loc, 'ab' if exist else 'wb') as csvfile:
                    df.to_csv(csvfile, header = not exist, index = False)
                    
                exist = True
            
    sys.stdout.flush()
    print(' Rosters: 100.00% Complete\n')