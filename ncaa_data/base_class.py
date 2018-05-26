from __future__ import print_function
from utils.url_req import url_req
from utils.print_update import print_update
from params import params
import parsers
import os
from requests.exceptions import ConnectionError, ConnectTimeout, ReadTimeout
import pandas as pd
from bs4 import BeautifulSoup
import re
from sys import stdout


class base_class(object):

    def __init__(self, schema_name, storage_dir, engine, data_type, href_frame, url_ids,
                 parse_file_types=None, scrape_table=None, seasons=None, divisions=None):
        self.schema_name = schema_name
        self.storage_dir = storage_dir
        self.engine = engine
        self.data_type = data_type
        self.url_frame = 'http://stats.ncaa.org{0}'.format(href_frame)
        self.url_ids = url_ids
        self.html_path = '{0}\\html\\{1}'.format(self.storage_dir, self.data_type)

        if parse_file_types:
            self.parse_file_types = parse_file_types
        else:
            self.parse_file_types = [data_type]
        if scrape_table: self.scrape_table = scrape_table
        if seasons: self.seasons = seasons
        if divisions: self.divisions = divisions


    def _create_storage_folder(self):
        if not os.path.exists(self.html_path): os.makedirs(self.html_path)

    def _define_all_webpages(self):
        if self.data_type == 'team_index':
            self.all_pages = set((x,y) for x in self.seasons for y in self.divisions)
        elif self.data_type == 'box_score':
            query = 'SELECT DISTINCT {0} FROM {1}.{2}'.format(self.url_ids[0] ,self.schema_name, self.scrape_table)
            result = self.engine.execute(query)
            self.all_pages = set((x, y) for x in [row[self.url_ids[0]] for row in result] for y in [1,2])
        else:
            query = 'SELECT DISTINCT {0} FROM {1}.{2}'\
                .format(', '.join(self.url_ids), self.schema_name, self.scrape_table)
            result = self.engine.execute(query)
            self.all_pages = set(tuple(row[key] for key in result.keys()) for row in result)

    def _define_scraped_webpages(self):
        self.scraped_pages = set(tuple(int(y) for y in x[:-5].split('_')) for x in os.listdir(self.html_path))

    def _define_remain_webpages(self):
        self.remain_pages = list(self.all_pages - self.scraped_pages)

        self.completed, self.total = len(self.scraped_pages), len(self.all_pages)

    def _scrape_remain_webpages(self):
        if len(self.remain_pages) > 0:
            write_path = '{0}\\{1}.html'.format(self.html_path, '{0}_{1}' if len(self.remain_pages[0]) > 1 else '{0}')

            for page in self.remain_pages:
                try:
                    resp = url_req(self.url_frame.format(*page))
                    with open(write_path.format(*page), 'wb') as f: f.write(resp)

                    self.completed += 1
                    print_update('Scrape', self.completed, self.total)
                except (ConnectionError, ConnectTimeout, ReadTimeout, UnboundLocalError):
                    continue

        stdout.flush()
        print('\t{0: >6} : {1: <14}'.format('Scrape', 'Complete'))

    def scrape_data(self):
        self._create_storage_folder()
        self._define_all_webpages()
        self._define_scraped_webpages()
        self._define_remain_webpages()
        self._scrape_remain_webpages()


    def _define_all_html_files(self):
        self.all_htmls = set(x for x in os.listdir(self.html_path)
            if os.path.getsize('{0}\\{1}'.format(self.html_path, x)) > 20480)

    def _define_parsed_html_files(self):
        query = 'SELECT DISTINCT {0} FROM {1}.{2}'.format(', '.join(self.url_ids), self.schema_name, self.data_type)
        try:
            result = self.engine.execute(query)
            self.parsed_htmls = set('{0}.html'.format('_'.join([str(y) for y in x])) for x in result)
        except ValueError as err:
            if str(err) == 'Table {0} not found'.format(self.data_type):
                self.parsed_htmls = set()
            else:
                raise err

    def _define_remain_html_files__single_parse(self):
        self.remain_htmls = list(self.all_htmls - self.parsed_htmls)

        self.completed, self.total = len(self.parsed_htmls), len(self.all_htmls)

    def _parse_remain_html_files__single_parse(self):
        if len(self.remain_htmls) > 0:
            for file_name in self.remain_htmls:
                with open('{0}\\{1}'.format(self.html_path, file_name), 'r') as html_file:
                    soup = BeautifulSoup(html_file.read(), 'lxml')

                if re.compile('something went wrong').search(soup.text): continue

                parser = getattr(parsers, self.data_type)
                parser(self.engine, self.data_type, self.schema_name, file_name, soup)

                self.completed += 1
                print_update('Parse', self.completed, self.total)

        stdout.flush()
        print('\t{0: >6} : {1: <14}'.format('Parse', 'Complete'))

    def _define_remain_html_files__multi_parse(self):
        self.remain_htmls = {key: self.parse_file_types for key in self.all_htmls}
        for file_type in self.parse_file_types:
            query = 'SELECT DISTINCT {0} FROM {1}.{2}'.format(', '.join(self.url_ids), self.schema_name, file_type)
            try:
                result = self.engine.execute(query)
                parsed_files = set('{0}.html'.format('_'.join([str(y) for y in x])) for x in result)
                for p_file in parsed_files:
                    self.remain_htmls[p_file] = [x for x in self.remain_htmls[p_file] if x != file_type]
            except ValueError as err:
                if str(err) == 'Table {0} not found'.format(file_type):
                    continue
                else:
                    raise err
        for key in [key for key in self.remain_htmls if self.remain_htmls[key] == []]: del self.remain_htmls[key]

        self.completed, self.total = len(self.all_htmls) - len(self.remain_htmls.keys()), len(self.all_htmls)

    def _parse_remain_html_files__multi_parse(self):
        if len(self.remain_htmls) > 0:
            for file_name in self.remain_htmls:
                with open('{0}\\{1}'.format(self.html_path, file_name), 'r') as html_file:
                    soup = BeautifulSoup(html_file.read(), 'lxml')

                if re.compile('something went wrong').search(soup.text): continue

                for parse_file_type in self.parse_file_types:
                    if parse_file_type in self.remain_htmls[file_name]:
                        getattr(parsers, parse_file_type)\
                            (self.engine, parse_file_type, self.schema_name, file_name, soup)

                self.completed += 1
                print_update('Parse', self.completed, self.total)

        stdout.flush()
        print('\t{0: >6} : {1: <14}'.format('Parse', 'Complete'))

    def parse_data(self):
        self._define_all_html_files()
        if len(self.parse_file_types) == 1:
            self._define_parsed_html_files()
            self._define_remain_html_files__single_parse()
            self._parse_remain_html_files__single_parse()
        else:
            self._define_remain_html_files__multi_parse()
            self._parse_remain_html_files__multi_parse()
