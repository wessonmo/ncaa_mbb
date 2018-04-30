from bs4 import BeautifulSoup
import requests
import time
from collections import OrderedDict
import re
import pandas as pd

def soupify(url):
    header = {'User-Agent':
    	'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.2 (KHTML, like Gecko) Chrome/22.0.1216.0 Safari/537.2'}
    
    for i in range(5):
        try:
            req = requests.get(url, headers = header, timeout = 10)
            break
        except:
            time.sleep(1)
            continue
     
    return BeautifulSoup(req.content, 'lxml')

#team parsers
def team_index(season, division):
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
    
    data = pd.DataFrame(data)
    
    if len(data) > 0: return data
    else: raise Exception('No data: {0}'.format(url))


def roster(season_id, school_id):
    url = 'http://stats.ncaa.org/team/{0}/roster/{1}'.format(school_id, season_id)
        
    soup = soupify(url)
    
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
    data['height'] = [x[3] if x[3] not in ['','-'] else None for x in players]
    data['class'] = [x[4] if x[4] not in ['','N/A'] else None for x in players]
    
    data = pd.DataFrame(data)
    data = data.loc[data.jersey.isin(range(100) + [None]) & (data.player_name != 'Use, Don\'t')]
    
    if len(data) > 0: return data
    else: raise Exception('No data: {0}'.format(url))

arena_re = re.compile('(?<=Name: ).*')
capacity_re = re.compile('(?<=Name: ).*')
record_re = re.compile('(?<=Record: )[0-9]+\-[0-9]+')
ot_re = re.compile('(?<=\()[0-9]+(?=OT\))')

def team_info(row):
    url = 'http://stats.ncaa.org/team/{0}/{1}'.format(row.school_id, row.season_id)
    soup = soupify(url)

    dict_ = {}
    
    if row.facilities == 'left_only':
        text = soup.find('div', {'id': 'facility_div'}).text
        
        arena = arena_re.search(text).group(0) if arena_re.search(text) else None
        capacity = capacity_re.search(text).group(0) if capacity_re.search(text) else None

        dict_['facilities'] = pd.DataFrame([[row.season_id, row.school_id, arena, capacity]],
            columns = ['season_id','school_id','arena','capacity'])
        
    if row.coaches == 'left_only':
        coaches = soup.find('div', {'id': 'head_coaches_div'}).find('fieldset')
        coaches = coaches.find_all('fieldset') if coaches.find('fieldset') else [coaches]

        data = OrderedDict()

        data['season_id'] = [row.season_id]*len(coaches)
        data['school_id'] = [row.school_id]*len(coaches)
        data['coach_id'] = [int(re.compile('[0-9]+').search(coach.find('a').get('href')).group(0)) for coach in coaches]
        data['coach_name'] = [coach.find('a').text for coach in coaches]
        data['record'] = [record_re.search(coach.text).group(0) for coach in coaches]

        dict_['coaches'] = pd.DataFrame(data)
        
    if row.schedules == 'left_only':
        schedule = soup.find('td', text = re.compile('schedule', re.I)).find_parent('table')
        games = schedule.find_all('tr', {'class': None})

        data = OrderedDict()
        data['season_id'] = [row.season_id]*len(games)
        data['school_id'] = [row.school_id]*len(games)
        data['date'] = [x.find_all('td')[0].text for x in games]
        data['game_id'] = [int(x.find_all('td')[2].find('a').get('href').split('/')[3].split('?')[0]) for x in games]
        data['opp_id'] = [int(x.find_all('td')[1].find('a').get('href').split('/')[2])
            if x.find_all('td')[1].find('a') else None for x in games]
        data['opp_text'] = [re.split('(?<=.)@',x.find_all('td')[1].get_text(strip = True))[0] for x in games]
        data['loc_text'] = [re.split('(?<=.)@',x.find_all('td')[1].get_text(strip = True))[1]
            if len(re.split('(?<=.)@',x.find_all('td')[1].get_text(strip = True))) > 1 else None for x in games]
        data['school_pts'] = [int(x.find_all('td')[2].text.strip()[2:].split(' ')[0]) for x in games]
        data['opp_pts'] = [int(x.find_all('td')[2].text.strip()[2:].split(' ')[2]) for x in games]
        data['ot'] = [int(ot_re.search(x.find_all('td')[2].text.strip()[2:]).group(0))
            if ot_re.search(x.find_all('td')[2].text.strip()[2:]) else 0 for x in games]

        dict_['schedules'] = pd.DataFrame(data)

    return dict_

# url = 'http://stats.ncaa.org/teams/22078'
# url = 'http://stats.ncaa.org/teams/10003'
# soup = soupify(url)


school_id_re = re.compile('(?<=team\/)[0-9]+(?=\/)')

#game parsers
def game_summary(game_id):
    url = 'http://stats.ncaa.org/game/period_stats/{0}'.format(game_id)

    soup = soupify(url)

    school_ids = soup.find('td', text = re.compile('total', re.I)).find_parent('table').find_all('tr', {'class': None})
    school_ids = [int(school_id_re.search(x.find('a').get('href')).group(0)) if x.find('a') else None for x in schools]
    
    stats = soup.find('td', text = 'Game Stats').find_parent('table').find('table').find_all('tr', {'class': None})
    stats = [[y.text for y in x.find_all('td')] for x in stats]
    
    for i, school_id in enumerate(school_ids):
        data = OrderedDict()
                        
        data['game_id'] = [game_id]
        data['school_id'] = [school_id]
        
        for stat in stats:
            stat_name = '_'.join([x.lower()[:5] for x in stat[0].split(' ')])
            data[stat_name] = [stat[1 + i] if len(stat) >= 2 + i else None]
        
        summary = pd.concat([summary,pd.DataFrame(data)]) if 'summary' in locals().keys() else pd.DataFrame(data)

    return summary


def box_score(game_id):
    for period in [1,2]:
        if 'period_df' in locals().keys(): del period_df
        
        url = 'http://stats.ncaa.org/game/box_score/{0}?period_no={1}'.format(game_id, period)
        
        soup = soupify(url)
        
        teams = soup.find_all('tr', {'class': 'heading'})
        
        if (len(teams) < 2) and period == 2: return box_df
        elif len(teams) < 2:
            url = 'http://stats.ncaa.org/game/index/{0}'.format(game_id)
            
            soup = soupify(url)
            
            teams = soup.find_all('tr', {'class': 'heading'})
            
            if len(teams) < 2:
                return pd.DataFrame([[game_id] + [None]*7],
                                columns = ['game_id','period','school_id','order','player_id','player_name','pos','min'])
            else: full_scrape = False
        
        if 'var_list' not in locals().keys():
            var_list = [x.text for x in
                        teams[0].find_parent('table').find('tr', {'class': 'grey_heading'}).find_all('th')]

        for team in teams:
            school_id = int(school_id_re.search(soup.find('a', text = team.text.strip()).get('href')).group(0))\
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
                if 'period_df' in locals().keys() else pd.DataFrame(data)
                
        box_df = pd.concat([box_df, period_df]) if 'box_df' in locals().keys() else period_df
                
    return box_df


def game_info(row):
    url, primary, cont = 'http://stats.ncaa.org/game/play_by_play/{0}'.format(row.game_id), True, True
        
    soup = soupify(url)
    
    if re.compile('something went wrong').search(soup.text):
        url, primary = 'http://stats.ncaa.org/game/index/{0}'.format(row.game_id), False
        
        soup = soupify(url)
        
        cont = True if not re.compile('something went wrong').search(soup.text) else False
    
    dict_ = {}
        
    if row.game_times == 'left_only':
        game_time = soup.find('td', text = re.compile('game date', re.I)).find_next().text if cont else None
        
        dict_['game_times'] = pd.DataFrame([[row.game_id, game_time]], columns = ['game_id','game_time'])            
    
    if row.officials == 'left_only':
        officials = soup.find('td', text = 'Officials:').find_next().text.strip() if cont else None

        dict_['officials'] = pd.DataFrame([[row.game_id, officials]], columns = ['game_id','officials'])

    if row.pbps == 'left_only':
        if not primary:
            dict_['pbps'] = pd.DataFrame([[row.game_id] + [None]*7], columns = ['game_id','period','time','school1_id','school1_event','score','school2_id','school2_event'])
        else:
            team_ids = [''] if re.compile('something went wrong').search(soup.text)\
                else soup.find('td', text = re.compile('total', re.I)).find_parent('table').find_all('tr', {'class': None})

            if len([x for x in team_ids if x.find('a') != None]) < 1:
                dict_['pbps'] = pd.DataFrame([[row.game_id] + [None]*7],
                    columns = ['game_id','period','time','school1_id','school1_event','score','school2_id','school2_event'])
                return dict_

            team_ids = [int(re.compile('(?<=team\/)[0-9]+(?=\/)').search(x.find('a').get('href')).group(0))
                if x.find('a') else None for x in team_ids]

            periods = set(x.get('href') for x in soup.find_all('a', href = re.compile('#pd[0-9]')))

            if periods == set():
                dict_['pbps'] = pd.DataFrame([[row.game_id] + [None]*7],
                    columns = ['game_id','period','time','school1_id','school1_event','score','school2_id','school2_event'])
                return dict_

            for period in periods:
                events = soup.find('a', {'id': period[1:]}).find_parent('table').find_next_sibling()\
                    .find_all('tr', {'class': None})
                    
                if events == []:
                    dict_['pbps'] = pd.DataFrame([[row.game_id] + [None]*7],
                        columns = ['game_id','period','time','school1_id','school1_event','score','school2_id','school2_event'])
                    return dict_
                    
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

            dict_['pbps'] = pbps
    
    return dict_