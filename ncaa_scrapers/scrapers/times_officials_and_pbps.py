#from __future__ import print_function
from functions.soupify import soupify
import os.path
import pandas as pd
from collections import OrderedDict
import re
#import sys

def scraper():
        
    needed = pd.read_csv('csv\\game_ids.csv', header = 0)
    needed = needed.loc[~pd.isnull(needed.game_id)][['game_id']].drop_duplicates()
    
    
    filenames = ['game_times','officials','pbps']
    
    for filename in filenames:
                
        file_loc = 'csv\\{0}.csv'.format(filename)
        
        if os.path.isfile(file_loc.format(filename)):
            
            file_df = pd.read_csv(file_loc, header = 0)[['game_id']].drop_duplicates()
            
            needed = pd.merge(needed, file_df, how = 'left', on = ['game_id'], indicator = filename)
            
        else:
            
            needed.loc[:,filename] = 'left_only'
            
    left = needed.loc[needed.apply(lambda x: 'left_only' in [x.game_times, x.officials, x.pbps],
                                                       axis = 1)]
        
#    completed = len(needed) - len(left)
    for index, row in left.sort_values('game_id').iterrows():
        
#        percent_complete = '%5.2f'%(float(completed)/len(needed)*100)
#        
#        sys.stdout.flush()
#        print('Game Times, Officials, and Pbps: {0}% Complete'.format(percent_complete), end = '\r')
        
        
        url = 'http://stats.ncaa.org/game/play_by_play/{0}'.format(row.game_id)
        
        soup = soupify(url)
        
        if re.compile('something went wrong').search(soup.text):
            
            url = 'http://stats.ncaa.org/game/box_score/{0}'.format(row.game_id)
            
            soup = soupify(url)
            
        
        dfs = []
        
        
        if row.game_times == 'left_only':
                        
            game_time = soup.find('td', text = re.compile('game date', re.I)).find_next().text
                    
                
            time = pd.DataFrame([[row.game_id, game_time]], columns = ['game_id','game_time'])
            
            dfs.append(('game_times',time))
            
            
        if row.officials == 'left_only':
            
            officials = soup.find('td', text = 'Officials:').find_next().text.strip()
            
            off = pd.DataFrame([[row.game_id, officials]], columns = ['game_id','officials'])
            
            dfs.append(('officials',off))
            
            
        if row.pbps == 'left_only':
            
            team_ids = [''] if re.compile('something went wrong').search(soup.text)\
                else soup.find('td', text = re.compile('total', re.I)).find_parent('table').find_all('tr', {'class': None})
                
                
            if len([x for x in team_ids if x.find('a') != None]) < 2:
                
                pbps = pd.DataFrame([[row.game_id] + [None]*7],
                    columns = ['game_id','period','time','school1_id','school1_event','score','school2_id','school2_event'])
                
            else:
                
                if 'pbps' in set(globals().keys()) & set(locals().keys()):
                    del pbps
                
                team_ids = [int(re.compile('(?<=team\/)[0-9]+(?=\/)').search(x.find('a').get('href')).group(0))
                    for x in team_ids]
                
                periods = set(x.get('href') for x in soup.find_all('a', href = re.compile('#pd[0-9]')))
                
                if periods == set():
                    
                    pbps = pd.DataFrame([[row.game_id] + [None]*7],
                        columns = ['game_id','period','time','school1_id','school1_event','score','school2_id',
                                   'school2_event'])
                    
                else:
                    
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
                        
                        if len(pd.DataFrame(data)) == 0:
                            
                            pbps = pd.DataFrame([[row.game_id] + [None]*7],
                                columns = ['game_id','period','time','school1_id','school1_event','score','school2_id',
                                           'school2_event'])
                        
                        elif 'pbps' not in set(globals().keys()) & set(locals().keys()):
                            
                            pbps = pd.DataFrame(data)
                            
                        else:
                            
                            pbps = pd.concat([pbps, pd.DataFrame(data)])
            
            dfs.append(('pbps',pbps))
            
            
        for filename2, df in dfs:
            
            file_loc2 = 'csv\\{0}.csv'.format(filename2)
            
            exist = os.path.isfile(file_loc2)
            
            with open(file_loc2, 'ab' if exist else 'wb') as csvfile:
                df.to_csv(csvfile, header = not exist, index = False)
                
        
#        completed += 1
#    
#    sys.stdout.flush()
#    print('Game Times, Officials, and Pbps: 100.00% Complete')
    
    
if __name__ == '__main__':
    
    scraper()