from print_status import print_status
from bs4 import BeautifulSoup
from collections import OrderedDict
import re
import pandas as pd
import os
import sqlalchemy


ncaa_err_re = re.compile('something went wrong')

#team parsers
def team_indexes(engine):
    file_type = 'Team Indexes'
    html_path = 'html\\team_index'
    
    all_files = set(os.listdir(html_path))
    try:
        sql = pd.read_sql_table('team_index', engine, schema = 'raw_data')
        parsed_files = set(sql.apply(lambda x: '{0}_{1}.html'.format(x.season, x.division), axis = 1))
    except ValueError as err:
        if str(err) == 'Table team_index not found':
            parsed_files = set()
        else:
            raise err
    remain_files = all_files - parsed_files
    
    completed, total = len(parsed_files), len(all_files)
    if len(remain_files) > 0:
        print_status(file_type, 'parse', completed, total, stage = 0)
        
        for file_ in remain_files:
            season, division = int(file_[:4]), int(file_[5])
            
            with open('{0}\\{1}'.format(html_path, file_), 'r') as html_file:
                soup = BeautifulSoup(html_file.read(), 'lxml')
                
            schools = soup.find_all('a', href = re.compile('\/team\/[0-9]+\/[0-9]+$'))
            
            data = OrderedDict()
    
            data['season'] = [season]*len(schools)
            data['season_id'] = [int(x.get('href').split('/')[-1]) for x in schools]
            data['school_id'] = [int(x.get('href').split('/')[2]) for x in schools]
            data['school_name'] = [x.text for x in schools]
            data['division'] = [division]*len(schools)
            
            data = pd.DataFrame(data)
            
            data.to_sql('team_index', engine, schema = 'raw_data', if_exists = 'append', index = False)
            
            completed += 1
            print_status(file_type, 'parse', completed, total, stage = 1)
    
    print_status(file_type, 'parse', completed, total, stage = 2)


arena_re = re.compile('(?<=Name: ).*')
capacity_re = re.compile('(?<=Capacity: ).*')
record_re = re.compile('(?<=Record: )[0-9]+\-[0-9]+')
ot_re = re.compile('(?<=\()[0-9]+(?=OT\))')

def team_info(engine):
    file_type = 'Team Info'
    derived_file_types = ['coaches', 'facilities', 'schedules']
    html_path = 'html\\team_info'
    
    all_files = set(os.listdir(html_path))
    
    remain_files = {key: derived_file_types for key in all_files}
    for d_file_type in derived_file_types:
        try:
            sql = pd.read_sql_table(d_file_type, engine, schema = 'raw_data')
            parsed_files = set(sql.apply(lambda x: '{0}_{1}.html'.format(x.season_id, x.school_id), axis = 1))
            for p_file in parsed_files: remain_files[p_file] = [x for x in remain_files[p_file] if x != d_file_type]
        except ValueError as err:
            if str(err) == 'Table {0} not found'.format(d_file_type):
                continue
            else:
                raise err
    for key in [key for key in remain_files if remain_files[key] == []]: del remain_files[key]
    
    completed, total = len(all_files) - len(remain_files), len(all_files)
    if len(remain_files) > 0:
        print_status(file_type, 'parse', completed, total, stage = 0)
        
        for file_ in remain_files:
            season_id, school_id = int(file_[:-5].split('_')[-2]), int(file_[:-5].split('_')[-1])
            
            with open('{0}\\{1}'.format(html_path, file_), 'r') as html_file:
                soup = BeautifulSoup(html_file.read(), 'lxml')
            
            
            if ncaa_err_re.search(soup.text): continue
            
            if 'facilities' in remain_files[file_]:
                fac_text = soup.find('div', {'id': 'facility_div'}).text
                
                data = OrderedDict()
                
                data['season_id'] = [season_id]
                data['school_id'] = [school_id]
                data['arena'] = [arena_re.search(fac_text).group(0) if arena_re.search(fac_text) else None]
                data['capacity'] = [int(re.sub(',', '', capacity_re.search(fac_text).group(0)))
                    if capacity_re.search(fac_text) else None]
                
                data = pd.DataFrame(data)
                
                data.to_sql('facilities', engine, schema = 'raw_data', if_exists = 'append', index = False)
            
            if 'coaches' in remain_files[file_]:
                coaches = soup.find('div', {'id': 'head_coaches_div'}).find('fieldset')
                coaches = coaches.find_all('fieldset') if coaches.find('fieldset') else [coaches]
        
                data = OrderedDict()
        
                data['season_id'] = [season_id]*len(coaches)
                data['school_id'] = [school_id]*len(coaches)
                data['coach_id'] = [int(re.compile('[0-9]+').search(coach.find('a').get('href')).group(0)) for coach in coaches]
                data['coach_name'] = [coach.find('a').text for coach in coaches]
                data['wins'] = [int(record_re.search(coach.text).group(0).split('-')[0]) for coach in coaches]
                data['losses'] = [int(record_re.search(coach.text).group(0).split('-')[1]) for coach in coaches]
        
                data = pd.DataFrame(data)
                
                data.to_sql('coaches', engine, schema = 'raw_data', if_exists = 'append', index = False)
            
            if 'schedules' in remain_files[file_]:
                schedule = soup.find('td', text = re.compile('schedule', re.I)).find_parent('table')
                games = [x for x in schedule.find_all('tr', {'class': None})
                    if len(x.find_all('td')) == 3 and x.find_all('td')[2].get_text(strip = True) != '-']
                
                game_ids = [int(x.find_all('td')[2].find('a').get('href').split('/')[3].split('?')[0]) for x in games]
        
                data = OrderedDict()
                data['season_id'] = [season_id]*len(games)
                data['school_id'] = [school_id]*len(games)
                data['date'] = [x.find_all('td')[0].text for x in games]
                data['game_id'] = game_ids
                data['opp_id'] = [int(x.find_all('td')[1].find('a').get('href').split('/')[2])
                    if x.find_all('td')[1].find('a') else None for x in games]
                data['opp_text'] = [re.sub('@ ','', re.split('(?<=.)@',x.find_all('td')[1].get_text(strip = True))[0])
                    for x in games]
                data['school_away'] = [True if '@' in re.split('(?<=.)@',x.find_all('td')[1].get_text(strip = True))[0]
                    else False for x in games]
                data['loc_text'] = [re.split('(?<=.)@',x.find_all('td')[1].get_text(strip = True))[1]
                    if re.compile('(?<=.)@').search(x.find_all('td')[1].get_text(strip = True)) else None for x in games]
                data['school_pts'] = [int(x.find_all('td')[2].text.strip()[2:].split(' ')[0]) for x in games]
                data['opp_pts'] = [int(x.find_all('td')[2].text.strip()[2:].split(' ')[2]) for x in games]
                data['ot'] = [int(ot_re.search(x.find_all('td')[2].text.strip()[2:]).group(0))
                    if ot_re.search(x.find_all('td')[2].text.strip()[2:]) else 0 for x in games]
        
                data = pd.DataFrame(data)
                
                data.to_sql('schedules', engine, schema = 'raw_data', if_exists = 'append', index = False)
            
            completed += 1
            print_status(file_type, 'parse', completed, total, stage = 1)
    
    print_status(file_type, 'parse', completed, total, stage = 2)


def rosters(engine):
    file_type = 'Rosters'
    html_path = 'html\\roster'
    
    all_files = set(os.listdir(html_path))
    try:
        sql = pd.read_sql_table('rosters', engine, schema = 'raw_data')
        parsed_files = set(sql.apply(lambda x: '{0}_{1}.html'.format(x.season_id, x.school_id), axis = 1))
    except ValueError as err:
            if str(err) == 'Table rosters not found':
                parsed_files = set()
            else:
                raise err
    remain_files = all_files - parsed_files
    
    completed, total = len(parsed_files), len(all_files)
    if len(remain_files) > 0:
        print_status(file_type, 'parse', completed, total, stage = 0)
        
        for file_ in remain_files:
            season_id, school_id = int(file_[:-5].split('_')[-2]), int(file_[:-5].split('_')[-1])
            
            with open('{0}\\{1}'.format(html_path, file_), 'r') as html_file:
                soup = BeautifulSoup(html_file.read(), 'lxml')
                
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
            data['height'] = [int(x[3].split('-')[0])*12 + int(x[3].split('-')[1])
                if x[3] not in ['','-'] else None for x in players]
            data['class'] = [x[4] if x[4] not in ['','N/A'] else None for x in players]
            
            data = pd.DataFrame(data)
            data = data.loc[data.jersey.isin(range(100) + [None]) & (data.player_name != 'Use, Don\'t')]
            
            data.to_sql('rosters', engine, schema = 'raw_data', if_exists = 'append', index = False)
            
            completed += 1
            print_status(file_type, 'parse', completed, total, stage = 1)
    
    print_status(file_type, 'parse', completed, total, stage = 2)


school_id_re = re.compile('(?<=team\/)[0-9]+(?=\/)')

#game parsers
def game_summaries(engine):
    file_type = 'Game Summaries'
    html_path = 'html\\summary'
    
    all_files = set(os.listdir(html_path))
    try:
        sql = pd.read_sql_table('game_summaries', engine, schema = 'raw_data')
        parsed_files = set(sql.game_id.apply(lambda x: '{0}.html'.format(x)))
    except ValueError as err:
            if str(err) == 'Table game_summaries not found':
                parsed_files = set()
            else:
                raise err
    remain_files = all_files - parsed_files
    
    completed, total = len(parsed_files), len(all_files)
    if len(remain_files) > 0:
        print_status(file_type, 'parse', completed, total, stage = 0)
        
        for file_ in remain_files:
            game_id = int(file_[:-5])
            
            with open('{0}\\{1}'.format(html_path, file_), 'r') as html_file:
                soup = BeautifulSoup(html_file.read(), 'lxml')
            
            table = soup.find('td', text = re.compile('total', re.I)).find_parent('table')
            school_ids = [int(school_id_re.search(x.find('a').get('href')).group(0))
                if x.find('a') else None for x in table.find_all('tr', {'class': None})]
            
            table = soup.find('td', text = 'Game Stats').find_parent('table').find('table')
            stats = [[y.text for y in x.find_all('td')] for x in table.find_all('tr', {'class': None})]
            
            for i, school_id in enumerate(school_ids):
                data = OrderedDict()
                                
                data['game_id'] = [game_id]
                data['school_id'] = [school_id]
                
                for stat in stats:
                    stat_name = '_'.join([x.lower()[:5] for x in stat[0].split(' ')])
                    data[stat_name] = [stat[1 + i] if len(stat) >= 2 + i else None]
                
                data = pd.DataFrame(data)
        
                data.to_sql('game_summaries', engine, schema = 'raw_data', if_exists = 'append', index = False)
            
            completed += 1
            print_status(file_type, 'parse', completed, total, stage = 1)
    
    print_status(file_type, 'parse', completed, total, stage = 2)


def box_scores(engine):    
    file_type = 'Box Scores'
    html_path = 'html\\box_score'
    
    all_files = set(os.listdir(html_path))
    try:
        sql = pd.read_sql_table('box_scores', engine, schema = 'raw_data')
        parsed_files = set(sql.apply(lambda x: '{0}_{1}.html'.format(x.game_id, x.period), axis = 1))
    except ValueError as err:
            if str(err) == 'Table box_scores not found':
                parsed_files = set()
            else:
                raise err
    remain_files = all_files - parsed_files
    
    completed, total = len(parsed_files), len(all_files)
    if len(remain_files) > 0:
        print_status(file_type, 'parse', completed, total, stage = 0)
        
        for file_ in remain_files:
            game_id, period = int(file_[:-5].split('_')[0]), int(file_[:-5].split('_')[1])
            
            with open('{0}\\{1}'.format(html_path, file_), 'r') as html_file:
                soup = BeautifulSoup(html_file.read(), 'lxml')
            
            if ncaa_err_re.search(soup.text): continue
    
            teams = [x.find_parent('table') for x in soup.find_all('tr', {'class': 'heading'})]
            
            table = soup.find('td', text = re.compile('total', re.I)).find_parent('table')
            school_ids = [int(school_id_re.search(x.find('a').get('href')).group(0)) if x.find('a') else None
                              for x in table.find_all('tr', {'class': None})]
                
            
            for i, team in enumerate(teams):
                school_name = team.find('td').get_text(strip = True)
    
                school_id = school_ids[i]
                
                players = [x.find_all('td') for x in team.find_all('tr', {'class': 'smtext'})]
                
                var_list = [x.text.lower() for x in team.find('tr', {'class': 'grey_heading'}).find_all('th')]
                
                data = OrderedDict()
                    
                data['game_id'] = [game_id]*len(players)
                data['period'] = [period]*len(players)
                data['school_name'] = [school_name]*len(players)
                data['school_id'] = [school_id]*len(players)
                data['order'] = range(len(players))
                data['player_id'] = [int(x[0].find('a').get('href').split('=')[-1]) if x[0].find('a') != None
                    else None for x in players]
                data['player_name'] = [re.sub(r'[^\x00-\x7F]+','',x[0].text.strip()) for x in players]
                data['pos'] = [x[1].text.strip() if x[1].text != '' else None for x in players]
                
                minutes_text = [x[2].text.strip().split(':') if x[2].text.strip() != '' else None for x in players]
                minute_format = 0 if sum([int(x[0]) for x in minutes_text if x]) else 1
                
                data['min'] = [int(x[minute_format]) if x else None for x in minutes_text]
                
                
                for j in range(3,18):
                    var_name = var_list[j]\
                        if ' ' not in var_list[j] else ''.join([var_list[j].split(' ')[0][0], var_list[j].split(' ')[1]])
                    var_name = var_name[:-1] if var_name[-1] == 's' else var_name
                    
                    data[var_name] = [int(x[j].text.strip()) if x[j].text.strip() != '' else None for x in players]
                    
                data = pd.DataFrame(data)
                
                data.to_sql('box_scores', engine, schema = 'raw_data', if_exists = 'append', index = False)
            
            completed += 1
            print_status(file_type, 'parse', completed, total, stage = 1)
    
    print_status(file_type, 'parse', completed, total, stage = 2)


def pbps(engine):
    file_type = 'Play-by-plays'
    derived_file_types = ['game_times', 'game_locations', 'officials', 'pbps']
    html_path = 'html\\pbp'
    
    all_files = set(os.listdir(html_path))
    
    remain_files = {key: derived_file_types for key in all_files}
    for d_file_type in derived_file_types:
        try:
            sql = pd.read_sql_table(d_file_type, engine, schema = 'raw_data')
            parsed_files = set(sql.game_id.apply(lambda x: '{0}.html'.format(x)))
            for p_file in parsed_files: remain_files[p_file] = [x for x in remain_files[p_file] if x != d_file_type]
        except ValueError as err:
            if str(err) == 'Table {0} not found'.format(d_file_type):
                continue
            else:
                raise err
    for key in [key for key in remain_files if remain_files[key] == []]: del remain_files[key]
    
    completed, total = len(all_files) - len(remain_files), len(all_files)
    if len(remain_files) > 0:
        print_status(file_type, 'parse', completed, total, stage = 0)
        
        for file_ in remain_files:
            game_id = int(file_[:-5])
            
            with open('{0}\\{1}'.format(html_path, file_), 'r') as html_file:
                soup = BeautifulSoup(html_file.read(), 'lxml')
            
            
            if ncaa_err_re.search(soup.text): continue
            
            if 'game_times' in remain_files[file_]:
                game_time = soup.find('td', text = 'Game Date:').find_next().text
                
                pd.DataFrame([[game_id, game_time]], columns = ['game_id','game_time'])\
                .to_sql('game_times', engine, schema = 'raw_data', if_exists = 'append', index = False,
                        dtype = {'game_time': sqlalchemy.types.VARCHAR})
            
            if 'game_locations' in remain_files[file_]:
                game_loc = soup.find('td', text = 'Location:').find_next().text if soup.find('td', text = 'Location:') else None
                
                pd.DataFrame([[game_id, game_loc]], columns = ['game_id','game_loc'])\
                .to_sql('game_locations', engine, schema = 'raw_data', if_exists = 'append', index = False)
            
            if 'officials' in remain_files[file_]:
                officials = soup.find('td', text = 'Officials:').find_next().text.strip()
                
                pd.DataFrame([[game_id, officials]], columns = ['game_id','officials'])\
                .to_sql('officials', engine, schema = 'raw_data', if_exists = 'append', index = False)
            
            if 'pbps' in remain_files[file_]:
                teams = [x.find('td') for x in
                         soup.find('td', text = re.compile('total', re.I)).find_parent('table').find_all('tr', {'class': None})]
        
                periods = set(x.get('href') for x in soup.find_all('a', href = re.compile('#pd[0-9]')))
        
                if periods == set(): raise ValueError('missing periods for pbp: {0}'.format(game_id))
        
                for period in periods:
                    events = soup.find('a', {'id': period[1:]}).find_parent('table').find_next_sibling()\
                        .find_all('tr', {'class': None})
                        
                    if events == []: continue
                        
                    data = OrderedDict()
                            
                    data['game_id'] = [game_id]*len(events)
                    data['period'] = [int(re.compile('[0-9]+').search(period).group(0))]*len(events)
                    data['time'] = [x.find_all('td')[0].text for x in events]
                    data['order'] = range(len(events))
                    data['school1_id'] = [int(teams[0].find('a').get('href').split('/')[2])
                        if teams[0].find('a') else None]*len(events)
                    data['school1_name'] = [teams[0].text]*len(events)
                    data['school1_event'] = [re.sub(r'[^\x00-\x7F]+','',x.find_all('td')[1].text)
                                            if x.find_all('td')[1].text != '' else None for x in events]
                    data['score'] = [x.find_all('td')[2].text for x in events]
                    data['school2_id'] = [int(teams[1].find('a').get('href').split('/')[2])
                        if teams[1].find('a') else None]*len(events)
                    data['school2_name'] = [teams[1].text]*len(events)
                    data['school2_event'] = [re.sub(r'[^\x00-\x7F]+','',x.find_all('td')[3].text)
                                            if x.find_all('td')[3].text != '' else None for x in events]
                    
                    data = pd.DataFrame(data)
                
                    data.to_sql('pbps', engine, schema = 'raw_data', if_exists = 'append', index = False)
            
            completed += 1
            print_status(file_type, 'parse', completed, total, stage = 1)
    
    print_status(file_type, 'parse', completed, total, stage = 2)