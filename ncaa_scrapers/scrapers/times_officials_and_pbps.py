from __future__ import print_function
from functions.soupify import soupify
import pandas as pd
from collections import OrderedDict
import re
import os
import multiprocessing as mp
import sys
import time


def time_scrape(row, soup):
    
    game_time = soup.find('td', text = re.compile('game date', re.I)).find_next().text
    
    
    return pd.DataFrame([[row.game_id, game_time]], columns = ['game_id','game_time'])


def officials_scrape(row, soup):
    
    officials = soup.find('td', text = 'Officials:').find_next().text.strip()
    
    
    return pd.DataFrame([[row.game_id, officials]], columns = ['game_id','officials'])


def pbp_scrape(row, soup):
    
    if 'pbps' in set(globals().keys()) & set(locals().keys()):
        del pbps
    
    
    team_ids = [''] if re.compile('something went wrong').search(soup.text)\
        else soup.find('td', text = re.compile('total', re.I)).find_parent('table').find_all('tr', {'class': None})
                
                
    if len([x for x in team_ids if x.find('a') != None]) < 1:
        
        return pd.DataFrame([[row.game_id] + [None]*7],
            columns = ['game_id','period','time','school1_id','school1_event','score','school2_id','school2_event'])
        
        
    team_ids = [int(re.compile('(?<=team\/)[0-9]+(?=\/)').search(x.find('a').get('href')).group(0))
        if x.find('a') else None for x in team_ids]
    
    
    periods = set(x.get('href') for x in soup.find_all('a', href = re.compile('#pd[0-9]')))
    
                  
    if periods == set():
        
        return pd.DataFrame([[row.game_id] + [None]*7],
            columns = ['game_id','period','time','school1_id','school1_event','score','school2_id','school2_event'])
    
    
    for period in periods:
        
        events = soup.find('a', {'id': period[1:]}).find_parent('table').find_next_sibling()\
            .find_all('tr', {'class': None})
            
        data = OrderedDict()
                
        data['game_id'] = [row.game_id]*len(events)
        data['period'] = [int(re.compile('[0-9]+').search(period).group(0))]*len(events)
        data['time'] = [x.find_all('td')[0].text for x in events]
        data['school1_id'] = [team_ids[0]]*len(events)
        data['school1_event'] = [re.sub(r'[^\x00-\x7F]+','',x.find_all('td')[1].text)
                                if x.find_all('td')[1].text != '' else None for x in events]
        data['score'] = [x.find_all('td')[2].text for x in events]
        data['school2_id'] = [team_ids[1]]*len(events)
        data['school2_event'] = [re.sub(r'[^\x00-\x7F]+','',x.find_all('td')[3].text)
                                if x.find_all('td')[3].text != '' else None for x in events]
        
        pbps = pd.concat([pbps, pd.DataFrame(data)]) if 'pbps' in set(globals().keys()) | set(locals().keys())\
            else pd.DataFrame(data)
    
    
    return pbps


def data_scrape(row):
    
    url, primary = 'http://stats.ncaa.org/game/play_by_play/{0}'.format(row.game_id), True
        
    soup = soupify(url)
    
    if re.compile('something went wrong').search(soup.text):
        
        url, primary = 'http://stats.ncaa.org/game/index/{0}'.format(row.game_id), False
        
        soup = soupify(url)
        
        cont = True if not re.compile('something went wrong').search(soup.text) else False
        
    
    dict_ = {}
    
    
    if row.game_times == 'left_only':
        
        dict_['game_times'] = time_scrape(row, soup) if cont else\
            pd.DataFrame({'game_id': [row.game_id], 'game_time': [None]})
    
    
    if row.officials == 'left_only':
        
       dict_['officials'] = officials_scrape(row, soup) if cont else\
           pd.DataFrame({'game_id': [row.game_id], 'officials': [None]})
    
    
    if row.pbps == 'left_only':
        
        dict_['pbps'] = pbp_scrape(row, soup) if primary else pd.DataFrame([[row.game_id] + [None]*7],
            columns = ['game_id','period','time','school1_id','school1_event','score','school2_id','school2_event'])
    
    
    return dict_


def update(game_ids):
    
    print(' Game Info (Start Times, Officials, and Pbps):', end = '\r')
    
    needed = pd.DataFrame({'game_id': list(game_ids)})
    
    filenames = ['game_times','officials','pbps']
    
    for filename in filenames:
                
        file_loc = 'csv\\{0}.csv'.format(filename)
        
        if os.path.isfile(file_loc.format(filename)):
            
            file_df = pd.read_csv(file_loc, header = 0)[['game_id']].drop_duplicates()
            
            needed = pd.merge(needed, file_df, how = 'left', on = ['game_id'], indicator = filename)
            
        else:
            
            needed.loc[:,filename] = 'left_only'
            
    scraped = needed.loc[needed.apply(lambda x: 'left_only' not in [x.game_times, x.officials, x.pbps], axis = 1)]
    
    left = needed.loc[needed.apply(lambda x: 'left_only' in [x.game_times, x.officials, x.pbps], axis = 1)]
    
    
    if len(left) > 0:
        
        finished = []
        
        
        for section in range(0, len(left), 10):
            
            chunk = left.iloc[section : section + 10]
            
            
            chunk_finished = []
            
            pool = mp.Pool(processes = mp.cpu_count() - 1)
        
            results = [pool.apply_async(data_scrape, args = (row), callback = chunk_finished.append)
                for idx, row in chunk.iterrows()]
            
            
            start = time.time()
            while len(chunk_finished) < len(chunk):
                
                if time.time() - start >= 600:
                    
                    for p in results: p.terminate()
                    
                    raise ValueError('chunk took too long')
                    
                percent_complete = '%5.2f'%(float(len(chunk_finished) + len(finished) + len(scraped))/len(needed)*100)
                
                sys.stdout.flush()
                print(' Game Info (Start Times, Officials, and Pbps): {0}% Complete'.format(percent_complete, section),
                      end = '\r')
                
                time.sleep(0.5)
                
            
            output = [p.get() for p in results]
            
            for dict_ in output:
                
                for filetype in dict_.keys():
                    
                    df = dict_[filetype]
                
                    file_loc2 = 'csv\\{0}.csv'.format(filetype)
                
                    exist = os.path.isfile(file_loc2)
                
                    with open(file_loc2, 'ab' if exist else 'wb') as csvfile:
                        df.to_csv(csvfile, header = not exist, index = False)
                    
                    
    sys.stdout.flush()
    print(' Game Info (Start Times, Officials, and Pbps): 100.00% Complete\n')