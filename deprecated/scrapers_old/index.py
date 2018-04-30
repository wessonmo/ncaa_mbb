from functions.soupify import soupify
from collections import OrderedDict
import re
import pandas as pd

def html_parse(season, division):
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
    
    if len(data) > 0:
        return pd.DataFrame(data)
    else:
        raise Exception('No data: {0}'.format(url))