#from __future__ import print_function
from functions.soupify import soupify
import pandas as pd
from collections import OrderedDict
import re
import os
#import sys

def scraper():
    
    needed = pd.read_csv('csv\\game_ids.csv', header = 0)
    needed = set(needed.loc[~pd.isnull(needed.game_id)].game_id)
    
    
    file_loc = 'csv\\game_summaries.csv'
    
    exist = os.path.isfile(file_loc)
    
    scraped = set() if not exist else set(pd.read_csv(file_loc, header = 0).game_id)
    
    left = needed - scraped
    
    
#    completed = len(scraped)
    for game_id in left:
        
#        percent_complete = '%5.2f'%(float(completed)/len(needed)*100)
#        
#        sys.stdout.flush()
#        print('Box Scores: {0}% Complete'.format(percent_complete), end = '\r')
        
        
        url = 'http://stats.ncaa.org/game/period_stats/{0}'.format(game_id)
            
        soup = soupify(url)
        try:
            school_ids = soup.find('td', text = re.compile('total', re.I)).find_parent('table')\
                .find_all('tr', {'class': None})
        except:
            raise ValueError(url)
        school_ids = [int(re.compile('(?<=team\/)[0-9]+(?=\/)').search(x.find('a').get('href')).group(0))
            if x.find('a') else 999999 for x in school_ids]
        
        stats = soup.find('td', text = 'Game Stats').find_parent('table').find('table').find_all('tr', {'class': None})
        stats = [[y.text for y in x.find_all('td')] for x in stats]
        
        
        for i in [0,1]:
            
            data = OrderedDict()
                            
            data['game_id'] = [game_id]
            data['school_id'] = [school_ids[i]]
            
            for stat in stats:
                
                stat_name = '_'.join([x.lower()[:5] for x in stat[0].split(' ')])
                
                data[stat_name] = [stat[1 + i] if len(stat) >= 2 + i else None]
                
                
            if i == 0:
                        
                summ = pd.DataFrame(data)
                
            else:
                
                summ = pd.concat([summ, pd.DataFrame(data)])
                
                
        with open(file_loc, 'ab' if exist else 'wb') as csvfile:
            summ.to_csv(csvfile, header = not exist, index = False)
                
        exist = True


if __name__ == '__main__':
    
    scraper()