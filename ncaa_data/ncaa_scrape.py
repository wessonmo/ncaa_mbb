import scrapers
import multiprocessing as mp
from sys import argv

def ncaa_scrape(seasons, divisions = [1]):
	team_indexes = scrapers.team_indexes(seasons, divisions)

	game_ids = mp.Pool().apply_async(target = scrapers.team_info, args = (team_indexes))

	rosters_proc = mp.Process(target = scrapers.rosters, args = (team_indexes))
	rosters_proc.start()

	game_ids.join()
	game_ids = game_ids.get()

	game_summaries_proc = mp.Process(target = scrapers.game_summaries, args = (game_ids))
	game_summaries_proc.start()

	box_scores_proc = mp.Process(target = scrapers.box_scores, args = (game_ids))
	box_scores_proc.start()

	game_info_proc = mp.Process(target = scrapers.game_info, args = (game_ids))
	game_info_proc.start()

if __name__ == '__main__':
	ncaa_scrape(range(2012, argv[1]))