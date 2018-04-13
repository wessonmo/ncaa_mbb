from __future__ import print_function
from functions.soupify import soupify
import pandas as pd
from collections import OrderedDict
import re
import os
import sys

needed = pd.read_csv('csv\\game_ids.csv', header = 0)
needed = set(needed.loc[~pd.isnull(needed.game_id)].game_id)


file_loc = 'csv\\box_scores.csv'

exist = os.path.isfile(file_loc)

scraped = set() if not exist else set(pd.read_csv(file_loc, header = 0).game_id)


left = needed - scraped

completed = len(scraped)
for game_id in left:
            
    sys.stdout.flush()
    perc_string = '%5.2f'%(float(completed)/len(needed)*100)
    print('Box Scores: {0}% Complete'.format(perc_string), end = '\r')
    
    
    for period in [1,2]:
        
        url = 'http://stats.ncaa.org/game/box_score/{0}?period_no={1}'.format(game_id, period)
        
        soup = soupify(url)
        
        teams = soup.find_all('tr', {'class': 'heading'})
        
        if (teams == []) and (period == 2):
            break
            
        elif teams == []:
            
            box_df = pd.DataFrame([[game_id] + [None]*7],
                              columns = ['game_id','period','school_id','order','player_id','player_name','pos','min'])
            
        else:
        
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
                data['min'] = [x[2].text.strip() if x[2].text.strip() != '' else None for x in players]
                
                for j in range(3,18):
                    var_name = re.sub(' ','_',var_list[j].lower())
                    data[var_name] = [int(x[j].text.strip()) if x[j].text.strip() != '' else None for x in players]
                    
                if 'box_df' not in set(globals().keys()) & set(locals().keys()):
                    
                    box_df = pd.DataFrame(data)
                    
                else:
                    
                    box_df = pd.concat([box_df, pd.DataFrame(data)])
                    
            
        with open(file_loc, 'ab' if exist else 'wb') as csvfile:
            box_df.to_csv(csvfile, header = not exist, index = False)
            
        exist = True
    
            
    completed += 1

sys.stdout.flush()
print('Box Scores: 100.00% Complete')