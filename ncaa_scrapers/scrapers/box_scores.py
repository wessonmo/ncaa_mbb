from __future__ import print_function
from functions.soupify import soupify
import pandas as pd
from collections import OrderedDict
import re
import os
import multiprocessing as mp
import sys
import time


def data_scrape(game_id, full_scrape):
    
    if 'box_df' in set(globals().keys()) | set(locals().keys()):
            del box_df
        
    for period in [1,2]:
        
        if 'period_df' in set(globals().keys()) | set(locals().keys()):
            del box_df
        
        url = 'http://stats.ncaa.org/game/box_score/{0}?period_no={1}'.format(game_id, period)
        
        soup = soupify(url)
        
        teams = soup.find_all('tr', {'class': 'heading'})
        
        if (teams, period) == ([], 2):
            
            return box_df
        
        elif teams == []:
            
            url = 'http://stats.ncaa.org/game/index/{0}'.format(game_id)
            
            soup = soupify(url)
            
            teams = soup.find_all('tr', {'class': 'heading'})
            
            if teams == []:
            
                return pd.DataFrame([[game_id] + [None]*7],
                                columns = ['game_id','period','school_id','order','player_id','player_name','pos','min'])
                
            else:
                
                full_scrape = False
        
        if 'var_list' not in set(globals().keys()) & set(locals().keys()):
            
            var_list = [x.text for x in
                        teams[0].find_parent('table').find('tr', {'class': 'grey_heading'}).find_all('th')]
        
        for team in teams:
            
            school_id = int(re.compile('(?<=team\/)[0-9]+(?=\/)').search(soup.find('a',
                            text = team.text.strip()).get('href')).group(0))\
                            if soup.find('a', text = team.text.strip()) != None else None
            
            players = [x.find_all('td') for x in team.find_parent('table').find_all('tr', {'class': 'smtext'})]
            
            data = OrderedDict()
                
            data['game_id'] = [game_id]*len(players)
            data['period'] = [period]*len(players)
            data['school_id'] = [school_id]*len(players)
            data['order'] = range(len(players))
            data['player_id'] = [int(x[0].find('a').get('href').split('=')[-1]) if x[0].find('a') != None
                else None for x in players]
            data['player_name'] = [re.sub(r'[^\x00-\x7F]+','',x[0].text.strip()) for x in players]
            data['pos'] = [x[1].text.strip() if x[1].text != '' else None for x in players]
            data['min'] = [x[2].text.strip() if x[2].text.strip() != '' else None for x in players]\
                if full_scrape else [None]*len(players)
            
            for j in range(3,18):
                var_name = re.sub(' ','_',var_list[j].lower())
                data[var_name] = [int(x[j].text.strip()) if x[j].text.strip() != '' else None for x in players]\
                    if full_scrape else [None]*len(players)
                        
            period_df = pd.concat([period_df, pd.DataFrame(data)])\
                if 'period_df' in set(globals().keys()) | set(locals().keys()) else pd.DataFrame(data)
                
        box_df = pd.concat([box_df, period_df]) if 'box_df' in set(globals().keys()) | set(locals().keys())\
                else period_df
                
    return box_df


def update(game_ids):
    
    print(' Box Scores:', end = '\r')
    
    
    file_loc = 'csv\\box_scores.csv'
    
    exist = os.path.isfile(file_loc)
    
    box_scores = pd.read_csv(file_loc, header = 0) if exist else pd.DataFrame(columns = ['game_id'])
    
    
    scraped = set(box_scores.game_id)
    
    needed = game_ids
    
    left = needed - scraped
    
    
    if left:
        
        finished = []
        
        
        pool = mp.Pool()
        
        results = [pool.apply_async(data_scrape, args = (x, True), callback = finished.append) for x in left]
        
        
        while len(finished) < len(left):
            
            percent_complete = '%5.2f'%(float(len(finished) + len(scraped))/len(needed)*100)
            
            sys.stdout.flush()
            print(' Box Scores: {0}% Complete'.format(percent_complete), end = '\r')
            
            time.sleep(0.05)
            
        
        output = [p.get() for p in results]
        
        for df in output:
            
            with open(file_loc, 'ab' if exist else 'wb') as csvfile:
                df.to_csv(csvfile, header = not exist, index = False)
                
            exist = True
            
    sys.stdout.flush()
    print(' Box Scores: 100.00% Complete\n')