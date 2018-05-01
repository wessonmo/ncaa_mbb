import scrapers
import multiprocessing as mp
from sys import argv
import data_summary

scrapers.team_indexes(range(2013, int(argv[1]) + 1), [1])

scrapers.rosters()

scrapers.team_info()

scrapers.game_summaries()

scrapers.box_scores()

scrapers.game_info()

# data_summary.teams()
# data_summary.games()