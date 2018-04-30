from __future__ import print_function
from functions.soupify import soupify
import pandas as pd
from collections import OrderedDict
import re
import os
import multiprocessing as mp
import sys


def time_scrape(row, soup):
    
    game_time = soup.find('td', text = re.compile('game date', re.I)).find_next().text
    
    
    return pd.DataFrame([[row.game_id, game_time]], columns = ['game_id','game_time'])


def officials_scrape(row, soup):
    
    officials = soup.find('td', text = 'Officials:').find_next().text.strip()
    
    
    return pd.DataFrame([[row.game_id, officials]], columns = ['game_id','officials'])


def pbp_scrape(row, soup):    
    
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
            
        if events == []:
            
            return pd.DataFrame([[row.game_id] + [None]*7],
                columns = ['game_id','period','time','school1_id','school1_event','score','school2_id','school2_event'])
            
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
        
        pbps = pd.concat([pbps, pd.DataFrame(data)]) if 'pbps' in locals().keys() else pd.DataFrame(data)
    
    
    return pbps


def data_scrape(row):
    
    url, primary, cont = 'http://stats.ncaa.org/game/play_by_play/{0}'.format(row.game_id), True, True
        
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


def multi_proc(left, scraped, num_ids, multi_proc_bool):
    
    finished = 0
    
    chunk_size = 5
    
    for section in range(0, len(left), chunk_size):
        
        percent_complete = '%5.2f'%(float(finished + scraped)/num_ids*100)
            
        sys.stdout.flush()
        print(' Game Info (Start Times, Officials, and Pbps): {0}% Complete'.format(percent_complete, section),
              end = '\r')
        
        chunk = left.iloc[section : section + chunk_size]
        
        
        pool = mp.Pool(maxtasksperchild = 5)
    
        results = [pool.apply_async(data_scrape, args = (row)) for idx, row in chunk.iterrows()]
        try:
            output = [p.get(timeout = 30) for p in results]
        except mp.TimeoutError:
            output = []
        
        
        for dict_ in output:
            
            for filetype in dict_.keys():
                
                df = dict_[filetype]
            
                file_loc2 = 'csv\\{0}{1}.csv'.format(filetype, '_multi' if multi_proc_bool else '_single')
            
                exist2 = os.path.isfile(file_loc2)
            
                with open(file_loc2, 'ab' if exist2 else 'wb') as csvfile:
                    df.to_csv(csvfile, header = not exist2, index = False)
        
        
        finished += len(output)


def single_proc(left, scraped, num_ids, multi_proc_bool):
    
    finished = 0
    
    for index, row in left.iterrows():
        
        percent_complete = '%5.2f'%(float(finished + scraped)/num_ids*100)
            
        sys.stdout.flush()
        print(' Game Info (Start Times, Officials, and Pbps): {0}% Complete'.format(percent_complete), end = '\r')
            
        dict_ = data_scrape(row)
            
        for filetype in dict_.keys():
            
            df = dict_[filetype]
        
            file_loc2 = 'csv\\{0}{1}.csv'.format(filetype, '_multi' if multi_proc_bool else '_single')
        
            exist2 = os.path.isfile(file_loc2)
        
            with open(file_loc2, 'ab' if exist2 else 'wb') as csvfile:
                df.to_csv(csvfile, header = not exist2, index = False)
            
        finished += 1


def update(game_ids_out, multi_proc_bool):
    
    print(' Game Info (Start Times, Officials, and Pbps):', end = '\r')
    
    needed = pd.DataFrame({'game_id': list(game_ids_out)})
    
    filenames = ['game_times','officials','pbps']
    
    for filename in filenames:
                
        file_loc = 'csv\\{0}{1}.csv'.format(filename, '_multi' if multi_proc_bool else '_single')
        
        if os.path.isfile(file_loc):
            
            file_df = pd.read_csv(file_loc, header = 0)[['game_id']].drop_duplicates()
            
            needed = pd.merge(needed, file_df, how = 'left', on = ['game_id'], indicator = filename)
            
        else:
            
            needed.loc[:,filename] = 'left_only'
            
    scraped = len(needed.loc[needed.apply(lambda x: 'left_only' not in [x.game_times, x.officials, x.pbps], axis = 1)])
    
    left = needed.loc[needed.apply(lambda x: 'left_only' in [x.game_times, x.officials, x.pbps], axis = 1)]
    
    
    if len(left) > 0:
        
        multi_proc(left, scraped, len(game_ids_out), multi_proc_bool) if multi_proc_bool\
            else single_proc(left, scraped, len(game_ids_out), multi_proc_bool)
    
    
    sys.stdout.flush()
    print(' Game Info (Start Times, Officials, and Pbps): 100.00% Complete\n')