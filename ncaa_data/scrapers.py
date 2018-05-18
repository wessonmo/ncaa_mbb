from print_status import print_status
import os
import re
import requests
import pandas as pd
from requests.exceptions import ConnectionError, ConnectTimeout, ReadTimeout
import time


def url_req(url):
    header = {'User-Agent': ('Mozilla/5.0 (Windows NT 6.1)'
                             'AppleWebKit/537.2 (KHTML, like Gecko)'
                             'Chrome/22.0.1216.0 Safari/537.2')}
    
    for i in range(5):
        try:
            req = requests.get(url, headers = header, timeout = 5 + i*5)
            if re.compile('access denied', re.I).search(req.content):
                time.sleep(300)
                continue
            break
        except (ConnectionError, ConnectTimeout, ReadTimeout) as to:
            if i == 4:
                raise to
            else:
                continue
     
    return req.content
    

#team scrapers
def team_indexes(seasons, divisions):
    file_type = 'Team Indexes'
    html_path = 'html\\team_index\\{0}_{1}.html'
    base_url = 'http://stats.ncaa.org/team/inst_team_list?academic_year={0}&conf_id=-1&division={1}&sport_code=MBB'
    
    all_indexes = set((x,y) for x in seasons for y in divisions)
    indexes_needed = [x for x in all_indexes if not os.path.exists(html_path.format(*x))]

    completed, total = len(all_indexes) - len(indexes_needed), len(all_indexes)
    if len(indexes_needed) > 0:
        print_status(file_type, 'scrape', completed, total, stage = 0)
    
        for season, division in indexes_needed:
            try:
                resp = url_req(base_url.format(season, division))
            except (ConnectionError, ConnectTimeout, ReadTimeout):
                continue
                
            if re.compile('internal server error').search(resp) == None:
                with open(html_path.format(season, division), 'wb') as f: f.write(resp)
            else: continue
            
            completed += 1
            print_status(file_type, 'scrape', completed, total, stage = 1)
    
    print_status(file_type, 'scrape', completed, total, stage = 2)
    

def team_info(engine):
    file_type = 'Team Info'
    html_path = 'html\\team_info'
    
    sql = pd.read_sql_table('team_index', engine, schema = 'raw_data')
    
    all_teams = set(zip(sql.season_id, sql.school_id))
    scraped_teams = set((int(x[:-5].split('_')[0]),int(x[:-5].split('_')[1])) for x in os.listdir(html_path))
    remain_teams = all_teams - scraped_teams

    completed, total = len(scraped_teams), len(all_teams)
    if len(remain_teams) > 0:
        print_status(file_type, 'scrape', completed, total, stage = 0)
    
        for season_id, school_id in remain_teams:
            try:
                resp = url_req('http://stats.ncaa.org/team/{0}/{1}'.format(school_id, season_id))
            except (ConnectionError, ConnectTimeout, ReadTimeout):
                continue
            
            if re.compile('internal server error').search(resp) == None:
                with open('{0}\\{1}_{2}.html'.format(html_path, season_id, school_id), 'wb') as f: f.write(resp)
            else: continue
            
            completed += 1
            print_status(file_type, 'scrape', completed, total, stage = 1)
    
    print_status(file_type, 'scrape', completed, total, stage = 2)


def rosters(engine):
    file_type = 'Rosters'
    html_path = 'html\\roster'
    
    sql = pd.read_sql_table('team_index', engine, schema = 'raw_data')
    
    all_teams = set(zip(sql.season_id, sql.school_id))
    scraped_teams = set((int(x[:-5].split('_')[0]),int(x[:-5].split('_')[1])) for x in os.listdir(html_path))
    remain_teams = all_teams - scraped_teams

    completed, total = len(scraped_teams), len(all_teams)
    if len(remain_teams) > 0:
        print_status(file_type, 'scrape', completed, total, stage = 0)
    
        for season_id, school_id in remain_teams:
            try:
                resp = url_req('http://stats.ncaa.org/team/{0}/roster/{1}'.format(school_id, season_id))
            except (ConnectionError, ConnectTimeout, ReadTimeout):
                continue
            
            if re.compile('internal server error').search(resp) == None:
                with open('{0}\\{1}_{2}.html'.format(html_path, season_id, school_id), 'wb') as f: f.write(resp)
            else: continue
            
            completed += 1
            print_status(file_type, 'scrape', completed, total, stage = 1)
    
    print_status(file_type, 'scrape', completed, total, stage = 2)


#game scrapers
def pbps(engine):
    file_type = 'Play-by-plays'
    html_path = 'html\\pbp'
    
    sql = pd.read_sql_table('schedules', engine, schema = 'raw_data')
    
    all_games = set(sql.loc[~pd.isnull(sql.game_id)].game_id)
    scraped_games = set(int(x[:-5]) for x in os.listdir(html_path))
    remain_games = all_games - scraped_games

    completed, total = len(scraped_games), len(all_games)
    if len(remain_games) > 0:
        print_status(file_type, 'scrape', completed, total, stage = 0)
    
        for game_id in remain_games:
            try:
                resp = url_req('http://stats.ncaa.org/game/play_by_play/{0}'.format(game_id))
            except (ConnectionError, ConnectTimeout, ReadTimeout):
                continue
            
            if re.compile('internal server error').search(resp) == None:
                with open('{0}\\{1}.html'.format(html_path, game_id), 'wb') as f: f.write(resp)
            else: continue
            
            completed += 1
            print_status(file_type, 'scrape', completed, total, stage = 1)
    
    print_status(file_type, 'scrape', completed, total, stage = 2)
    

def game_summaries(engine):
    file_type = 'Game Summaries'
    html_path = 'html\\summary'
    
    sql = pd.read_sql_table('schedules', engine, schema = 'raw_data')
    
    all_games = set(sql.loc[~pd.isnull(sql.game_id)].game_id)
    scraped_games = set(int(x[:-5]) for x in os.listdir(html_path))
    remain_games = all_games - scraped_games

    completed, total = len(scraped_games), len(all_games)
    if len(remain_games) > 0:
        print_status(file_type, 'scrape', completed, total, stage = 0)
    
        for game_id in remain_games:
            try:
                resp = url_req('http://stats.ncaa.org/game/period_stats/{0}'.format(game_id))
            except (ConnectionError, ConnectTimeout, ReadTimeout):
                continue
            
            if re.compile('internal server error').search(resp) == None:
                with open('{0}\\{1}.html'.format(html_path, game_id), 'wb') as f: f.write(resp)
            else: continue
            
            completed += 1
            print_status(file_type, 'scrape', completed, total, stage = 1)
    
    print_status(file_type, 'scrape', completed, total, stage = 2)


def box_scores(engine):
    file_type = 'Box Scores'
    html_path = 'html\\box_score'
        
    sql = pd.read_sql_table('schedules', engine, schema = 'raw_data')
    
    all_halves = set((x, y) for x in set(sql.loc[~pd.isnull(sql.game_id)].game_id) for y in [1,2])
    scraped_halves = set((int(x[:-5].split('_')[0]), int(x[:-5].split('_')[1])) for x in os.listdir(html_path))
    remain_halves = all_halves - scraped_halves

    completed, total = len(scraped_halves), len(all_halves)
    if len(remain_halves) > 0:
        print_status(file_type, 'scrape', completed, total, stage = 0)
    
        for game_id, period in remain_halves:
            try:
                resp = url_req('http://stats.ncaa.org/game/box_score/{0}?period_no={1}'.format(game_id, period))
            except (ConnectionError, ConnectTimeout, ReadTimeout):
                continue
            
            if re.compile('internal server error').search(resp) == None:
                with open('{0}\\{1}_{2}.html'.format(html_path, game_id, period), 'wb') as f: f.write(resp)
            else: continue
            
            completed += 1
            print_status(file_type, 'scrape', completed, total, stage = 1)
    
    print_status(file_type, 'scrape', completed, total, stage = 2)