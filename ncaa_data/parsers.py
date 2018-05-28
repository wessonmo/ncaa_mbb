from collections import OrderedDict
import re
import pandas as pd
import sqlalchemy
from params import params

arena_re = re.compile('(?<=Name: ).*')
capacity_re = re.compile('(?<=Capacity: ).*')
record_re = re.compile('(?<=Record: )[0-9]+\-[0-9]+')
ot_re = re.compile('(?<=\()[0-9]+(?=OT\))')
school_id_re = re.compile('(?<=team\/)[0-9]+(?=\/)')

def team_index(engine, data_type, schema_name, file_name, soup):
    season, division = int(file_name[:4]), int(file_name[5])

    schools = soup.find_all('a', href = re.compile('\/team\/[0-9]+\/[0-9]+$'))

    data = OrderedDict()
    data['season'] = [season]*len(schools)
    data['season_id'] = [int(x.get('href').split('/')[-1]) for x in schools]
    data['school_id'] = [int(x.get('href').split('/')[2]) for x in schools]
    data['school_name'] = [x.text for x in schools]
    data['division'] = [division]*len(schools)
    data = pd.DataFrame(data)

    data.to_sql(data_type, engine, schema = schema_name, if_exists = 'append', index = False)

def conference(engine, data_type, schema_name, file_name, soup):
    school_id, seasons = int(file_name[:-5]), range(params['mbb']['min_season'], params['mbb']['max_season'] + 1)

    table = soup.find('table', {'id': 'team_history_data_table'}).find('tbody')
    rows = [x for x in table.find_all('tr') if int(x.find_all('td')[0].text[:4]) + 1 in seasons]
    
    data = OrderedDict()
    data['season'] = [x for x in reversed(seasons)]
    data['school_id'] = [school_id]*len(rows)
    data['conference'] = [x.find_all('td')[3].text for x in rows]
    data = pd.DataFrame(data)

    data.to_sql(data_type, engine, schema = schema_name, if_exists = 'append', index = False)

def facility(engine, data_type, schema_name, file_name, soup):
    season_id, school_id = int(file_name[:-5].split('_')[-2]), int(file_name[:-5].split('_')[-1])

    fac_text = soup.find('div', {'id': 'facility_div'}).text

    data = OrderedDict()
    data['season_id'] = [season_id]
    data['school_id'] = [school_id]
    data['arena'] = [arena_re.search(fac_text).group(0) if arena_re.search(fac_text) else None]
    data['capacity'] = [int(re.sub(',', '', capacity_re.search(fac_text).group(0)))
        if capacity_re.search(fac_text) else None]
    data = pd.DataFrame(data)

    data.to_sql(data_type, engine, schema = schema_name, if_exists = 'append', index = False)

def coach(engine, data_type, schema_name, file_name, soup):
    season_id, school_id = int(file_name[:-5].split('_')[-2]), int(file_name[:-5].split('_')[-1])

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

    data.to_sql(data_type, engine, schema = schema_name, if_exists = 'append', index = False)

def schedule(engine, data_type, schema_name, file_name, soup):
    season_id, school_id = int(file_name[:-5].split('_')[-2]), int(file_name[:-5].split('_')[-1])

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

    data.to_sql(data_type, engine, schema = schema_name, if_exists = 'append', index = False)

def roster(engine, data_type, schema_name, file_name, soup):
    season_id, school_id = int(file_name[:-5].split('_')[-2]), int(file_name[:-5].split('_')[-1])

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

    data.to_sql(data_type, engine, schema = schema_name, if_exists = 'append', index = False)

def summary(engine, data_type, schema_name, file_name, soup):
    game_id = int(file_name[:-5])

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

        data.to_sql(data_type, engine, schema = schema_name, if_exists = 'append', index = False)

def box_score(engine, data_type, schema_name, file_name, soup):
    game_id, period = int(file_name[:-5].split('_')[0]), int(file_name[:-5].split('_')[1])

    teams = [x.find_parent('table') for x in soup.find_all('tr', {'class': 'heading'})]

    table = soup.find('td', text = re.compile('total', re.I)).find_parent('table')
    school_ids = [int(school_id_re.search(x.find('a').get('href')).group(0)) if x.find('a') else None
                      for x in table.find_all('tr', {'class': None})]


    for i, team in enumerate(teams):
        school_name = team.find('td').get_text(strip = True)

        school_id = school_ids[i]

        players = [x.find_all('td') for x in team.find_all('tr', {'class': 'smtext'})]
        if players == []:
            query = 'INSERT INTO {0}.{1} (game_id, period, school_id) VALUES ({2}, {3}, {4})'\
                .format(schema_name, data_type, game_id, period, school_id)
            engine.execute(query)
            continue

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
            try:
                var_name = var_list[j]\
                    if ' ' not in var_list[j] else ''.join([var_list[j].split(' ')[0][0], var_list[j].split(' ')[1]])
            except:
                raise ValueError(game_id, period)
            var_name = var_name[:-1] if var_name[-1] == 's' else var_name
            data[var_name] = [int(x[j].text.strip()) if x[j].text.strip() != '' else None for x in players]
        data = pd.DataFrame(data)

        data.to_sql(data_type, engine, schema = schema_name, if_exists = 'append', index = False)

def game_time(engine, data_type, schema_name, file_name, soup):
    game_id = int(file_name[:-5])

    game_time = soup.find('td', text = 'Game Date:').find_next().text

    data = pd.DataFrame([[game_id, game_time]], columns = ['game_id','game_time'])

    data.to_sql(data_type, engine, schema = schema_name, if_exists = 'append', index = False,
                dtype = {'game_time': sqlalchemy.types.VARCHAR})

def game_location(engine, data_type, schema_name, file_name, soup):
    game_id = int(file_name[:-5])

    game_loc = soup.find('td', text = 'Location:').find_next().text if soup.find('td', text = 'Location:') else None

    data = pd.DataFrame([[game_id, game_loc]], columns = ['game_id','game_loc'])

    data.to_sql(data_type, engine, schema = schema_name, if_exists = 'append', index = False)

def officials(engine, data_type, schema_name, file_name, soup):
    game_id = int(file_name[:-5])

    officials = soup.find('td', text = 'Officials:').find_next().text.strip()

    data = pd.DataFrame([[game_id, officials]], columns = ['game_id','officials'])

    data.to_sql(data_type, engine, schema = schema_name, if_exists = 'append', index = False)

def pbp(engine, data_type, schema_name, file_name, soup):
    game_id = int(file_name[:-5])

    teams = [x.find('td') for x in
             soup.find('td', text = re.compile('total', re.I)).find_parent('table').find_all('tr', {'class': None})]

    periods = set(x.get('href') for x in soup.find_all('a', href = re.compile('#pd[0-9]')))

    if periods == set(): raise ValueError('missing periods')

    for period in periods:
        events = soup.find('a', {'id': period[1:]}).find_parent('table').find_next_sibling()\
            .find_all('tr', {'class': None})

        if events == []: raise ValueError('missing events')

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

        data.to_sql(data_type, engine, schema = schema_name, if_exists = 'append', index = False)
