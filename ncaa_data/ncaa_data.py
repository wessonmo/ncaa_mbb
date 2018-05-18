import scrapers
import parsers
#import multiprocessing as mp
from sys import argv
import sqlalchemy
#import data_summary

def ncaa_data_scrape(max_season):
    engine = sqlalchemy.create_engine('postgresql://postgres:@localhost:5432/ncaa_mbb')
    engine.execute('create schema if not exists raw_data;')
    
    scrapers.team_indexes(range(2013, max_season + 1), [1])
    parsers.team_indexes(engine)
    
    scrapers.team_info(engine)
    scrapers.rosters(engine)
    
    parsers.team_info(engine)
    
    scrapers.pbps(engine)
    scrapers.box_scores(engine)
    scrapers.game_summaries(engine)
    
    parsers.rosters(engine)
    parsers.game_summaries(engine)
    parsers.box_scores(engine)
    parsers.pbps(engine)
    
#    data_summary.teams()
#    data_summary.games()

if __name__ == '__main__':
    ncaa_data_scrape(int(argv[1]))