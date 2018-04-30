from __future__ import print_function
from functions.soupify import soupify
import pandas as pd
from collections import OrderedDict
import re
import os
import multiprocessing as mp
import sys

def scrape_game_info(game_id):
	pass

def scrape_box_scores(game_id):
	pass

def scrape_game_summaries(game_id):
	pass

def game_index(game_queue):
	game_file_types = {'game_info': {'game_times': [None], 'officials': [None], 'pbps': [None]},
		'box_scores': [None], 'game_summaries': [None]}
    game_dict = manager.dict(game_file_types)

	for file_type in game_dict.keys():
		for file_name in game_dict[file_type].keys():
			file_loc = 'csv\\{0}.csv'.format(file_name)
			file_exist = os.path.isfile(file_loc)

			game_dict[file_type][file_name] = list(pd.read_csv(file_loc).game_id.unique()) if file_exist else [None]

	while True:
		game_ids, poisoned = [x for x in game_queue.get() if not x not in ['stop','exit']], 'exit' in game_queue.get()

		for file_type in game_dict.keys():
			for file_name in game_dict[file_type].keys():
				pass

		if poisoned: break