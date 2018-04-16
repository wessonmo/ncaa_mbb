from soupify import soupify
from collections import OrderedDict
import re
import os.path
import pandas as pd

def scrape(season, division):
        
    url = 'http://stats.ncaa.org/team/inst_team_list?academic_year={0}&conf_id=-1&division={1}&sport_code=MBB'\
        .format(season, division)
    
    soup = soupify(url)
    
    schools = soup.find_all('a', href = re.compile('\/team\/[0-9]+\/[0-9]+$'))
    
    data = OrderedDict()
    
    data['season'] = [season]*len(schools)
    data['season_id'] = [int(x.get('href').split('/')[-1]) for x in schools]
    data['school_id'] = [int(x.get('href').split('/')[2]) for x in schools]
    data['school_name'] = [x.text for x in schools]
    data['division'] = [division]*len(schools)
    
    return data

def data_check(file_loc, seasons, divisions):
    
    exist = os.path.isfile(file_loc)
        
    index = pd.read_csv(file_loc, header = 0) if exist else pd.DataFrame(columns = ['season', 'division'])
        
    needed = set((x,y) for x in seasons for y in divisions) - set(zip(index.season, index.division))
    
    return needed