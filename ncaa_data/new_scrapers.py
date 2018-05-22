from __future__ import print_function
from utils.url_req import url_req
from utils.print_update import print_update
import new_parsers
import os
from requests.exceptions import ConnectionError, ConnectTimeout, ReadTimeout
from abc import ABCMeta
import pandas as pd
from bs4 import BeautifulSoup
import re
from sys import stdout

class data_obj(object):

    __metaclass__ = ABCMeta

    base_url = ''
    scrape_table = ''
    scrape_ids = []

    def __init__(self, schema_name, storage_dir, engine, data_type):
        self.data_type = data_type
        self.schema_name = schema_name
        self.storage_dir = storage_dir
        self.engine = engine
        self.html_path = '{0}\\html\\{1}'.format(self.storage_dir, self.data_type)
        self.parse_ids = self.scrape_ids


    def _create_storage_folder(self):
        if not os.path.exists(self.html_path): os.makedirs(self.html_path)

    def _define_all_pages(self):
        sql = pd.read_sql_table(self.scrape_table, self.engine, schema = self.schema_name)
        if len(self.scrape_ids) == 1:
            self.all_pages = set(sql[self.scrape_ids[0]])
        elif len(self.scrape_ids) == 2:
            self.all_pages = set(zip(sql[self.scrape_ids[0]], sql[self.scrape_ids[1]]))
        else:
            raise ValueError('too many parameters')

    def _define_scraped_pages(self):
        if len(self.scrape_ids) == 1:
            self.scraped_pages = set(x[:-5] for x in os.listdir(self.html_path))
        elif len(self.scrape_ids) == 2:
            self.scraped_pages = set((int(y), int(z)) for y, z in
                                     [x[:-5].split('_') for x in os.listdir(self.html_path)])
        else:
            raise ValueError('too many parameters')


    def _define_remain_pages(self):
        self.remain_pages = list(self.all_pages - self.scraped_pages)
        
        self.completed, self.total = len(self.scraped_pages), len(self.all_pages)

    def _scrape_remain_pages(self):
        if len(self.remain_pages) > 0:
            is_tuple = isinstance(self.remain_pages[0], tuple)
            write_path = '{0}\\{1}.html'.format(self.html_path, '{0}_{1}' if is_tuple else '{0}')

            for page in self.remain_pages:
                try:
                    if is_tuple:
                        resp = url_req(self.base_url.format(*page))
                        with open(write_path.format(*page), 'wb') as f: f.write(resp)
                    else:
                        resp = url_req(self.base_url.format(page))
                        with open(write_path.format(page), 'wb') as f: f.write(resp)

                    self.completed += 1
                    print_update('Scrape', self.completed, self.total)
                except (ConnectionError, ConnectTimeout, ReadTimeout, UnboundLocalError):
                    continue

        stdout.flush()
        print('\t{0: >6} : {1: <14}'.format('Scrape', 'Complete'))

    def scrape_data(self):
        self._create_storage_folder()
        self._define_all_pages()
        self._define_scraped_pages()
        self._define_remain_pages()
        self._scrape_remain_pages()


    def _define_all_htmls(self):
        self.all_htmls = set(os.listdir(self.html_path))

    def _define_parsed_htmls(self):
        try:
            sql = pd.read_sql_table(self.data_type, self.engine, schema = self.schema_name)
            pre_string =  '{0}.html'.format('{0}_{1}' if len(self.parse_ids) == 2 else '{0}')
            self.parsed_htmls = set(sql.apply(lambda x: pre_string.format(*x[self.parse_ids]), axis = 1))
        except ValueError as err:
            if str(err) == 'Table {0} not found'.format(self.data_type):
                self.parsed_htmls = set()
            else:
                raise err

    def _define_remain_htmls(self):
        self.remain_htmls = list(self.all_htmls - self.parsed_htmls)

        self.completed, self.total = len(self.parsed_htmls), len(self.all_htmls)

    def _parse_remain_htmls(self):
        if len(self.remain_htmls) > 0:
            for file_name in self.remain_htmls:
                with open('{0}\\{1}'.format(self.html_path, file_name), 'r') as html_file:
                    soup = BeautifulSoup(html_file.read(), 'lxml')

                if re.compile('something went wrong').search(soup.text): continue

                self._parser(file_name, soup)

                self.completed += 1
                print_update('Parse', self.completed, self.total)

        stdout.flush()
        print('\t{0: >6} : {1: <14}'.format('Parse', 'Complete'))

    def _parser(self, file_name, soup):
        parser = None
        parser(self.engine, self.data_type, self.schema_name, file_name, soup)

    def parse_data(self):
        self._define_all_htmls()
        self._define_parsed_htmls()
        self._define_remain_htmls()
        self._parse_remain_htmls()


class team_index(data_obj):

    base_url = 'http://stats.ncaa.org/team/inst_team_list?academic_year={0}&conf_id=-1&division={1}&sport_code=MBB'

    scrape_ids = ['season', 'division']


    def __init__(self, schema_name, storage_dir, engine, data_type, seasons, divisions):
        data_obj.__init__(self, schema_name, storage_dir, engine, data_type)
        self.seasons = seasons
        self.divisions = divisions


    def _define_all_pages(self):
        self.all_pages = set((x,y) for x in self.seasons for y in self.divisions)


    def _parser(self, file_name, soup):
        parser = new_parsers.parse_team_index
        parser(self.engine, self.data_type, self.schema_name, file_name, soup)


class team_info(data_obj):

    base_url = 'http://stats.ncaa.org/team/{1}/{0}'
    scrape_table = 'team_index'
    scrape_ids = ['season_id', 'school_id']


    def _define_remain_htmls(self):
        derived_file_types = ['coach', 'facility', 'schedule']

        self.remain_htmls = {key: derived_file_types for key in self.all_htmls}
        for d_file_type in derived_file_types:
            try:
                sql = pd.read_sql_table(d_file_type, self.engine, schema = self.schema_name)
                parsed_files = set(sql.apply(lambda x: '{0}_{1}.html'.format(*x[self.scrape_ids]), axis = 1))
                for p_file in parsed_files:
                    self.remain_htmls[p_file] = [x for x in self.remain_htmls[p_file] if x != d_file_type]
            except ValueError as err:
                if str(err) == 'Table {0} not found'.format(d_file_type):
                    continue
                else:
                    raise err
        for key in [key for key in self.remain_htmls if self.remain_htmls[key] == []]: del self.remain_htmls[key]

        self.completed, self.total = len(self.all_htmls) - len(self.remain_htmls.keys()), len(self.all_htmls)


    def _parse_remain_htmls(self):
        if len(self.remain_htmls) > 0:
            for file_name in self.remain_htmls:
                with open('{0}\\{1}'.format(self.html_path, file_name), 'r') as html_file:
                    soup = BeautifulSoup(html_file.read(), 'lxml')

                if re.compile('something went wrong').search(soup.text): continue

                for parse_data_type in ['facility', 'coach', 'schedule']:
                    if parse_data_type in self.remain_htmls[file_name]:
                        getattr(new_parsers, 'parse_{0}'.format(parse_data_type))\
                            (self.engine, parse_data_type, self.schema_name, file_name, soup)

                self.completed += 1
                print_update('Parse', self.completed, self.total)


class roster(data_obj):

    base_url = 'http://stats.ncaa.org/team/{1}/roster/{0}'
    scrape_table = 'team_index'
    scrape_ids = ['season_id', 'school_id']


    def _parser(self, file_name, soup):
        parser = new_parsers.parse_roster
        parser(self.engine, self.data_type, self.schema_name, file_name, soup)


class summary(data_obj):

    base_url = 'http://stats.ncaa.org/game/period_stats/{0}'
    scrape_table = 'schedule'
    scrape_ids = ['game_id']


    def _parser(self, file_name, soup):
        parser = new_parsers.parse_summary
        parser(self.engine, self.data_type, self.schema_name, file_name, soup)


class box_score(data_obj):

    base_url = 'http://stats.ncaa.org/game/box_score/{0}?period_no={1}'
    scrape_table = 'schedule'
    scrape_ids = ['game_id', 'period']


    def _define_all_pages(self):
        sql = pd.read_sql_table(self.scrape_table, self.engine, schema = self.schema_name)
        self.all_pages = set((x, y) for x in set(sql[self.scrape_ids[0]]) for y in [1,2])

    def _parser(self, file_name, soup):
        parser = new_parsers.parse_box_score
        parser(self.engine, self.data_type, self.schema_name, file_name, soup)


class pbp(data_obj):

    base_url = 'http://stats.ncaa.org/game/play_by_play/{0}'
    scrape_table = 'schedule'
    scrape_ids = ['game_id']


    def _define_remain_htmls(self):
        derived_file_types = ['game_time', 'game_location', 'officials', 'pbp']

        self.remain_htmls = {key: derived_file_types for key in self.all_htmls}
        for d_file_type in derived_file_types:
            try:
                sql = pd.read_sql_table(d_file_type, self.engine, schema = self.schema_name)
                parsed_files = set(sql.apply(lambda x: '{0}.html'.format(x[self.scrape_ids[0]]), axis = 1))
                for p_file in parsed_files:
                    self.remain_htmls[p_file] = [x for x in self.remain_htmls[p_file] if x != d_file_type]
            except ValueError as err:
                if str(err) == 'Table {0} not found'.format(d_file_type):
                    continue
                else:
                    raise err
        for key in [key for key in self.remain_htmls if self.remain_htmls[key] == []]: del self.remain_htmls[key]

        self.completed, self.total = len(self.all_htmls) - len(self.remain_htmls.keys()), len(self.all_htmls)

    def _parse_remain_htmls(self):
        if len(self.remain_htmls) > 0:
            for file_name in self.remain_htmls:
                with open('{0}\\{1}'.format(self.html_path, file_name), 'r') as html_file:
                    soup = BeautifulSoup(html_file.read(), 'lxml')

                if re.compile('something went wrong').search(soup.text): continue

                for parse_data_type in ['game_time', 'game_location', 'officials', 'pbp']:
                    if parse_data_type in self.remain_htmls[file_name]:
                        try:
                            getattr(new_parsers, 'parse_{0}'.format(parse_data_type))\
                                (self.engine, parse_data_type, self.schema_name, file_name, soup)
                        except ValueError as err:
                            if re.compile('missing (period|event)s').search(str(err)):
                                continue
                            else:
                                raise err

                self.completed += 1
                print_update('Parse', self.completed, self.total)
