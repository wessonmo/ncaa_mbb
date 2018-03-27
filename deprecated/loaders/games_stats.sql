begin;

drop table if exists mbb.games_stats;

create table mbb.games_stats (
	game_date	date,
	low_id		float,
	high_id		float,
	low_teff	float,
	low_efg		float,
	low_ptapm	float,
	low_astp	float,
	low_blkp	float,
	low_rbp		float,
	high_teff	float,
	high_efg	float,
	high_ptapm	float,
	high_astp	float,
	high_blkp	float,
	high_rbp	float,
	primary key (game_date, low_id, high_id)
);

truncate table mbb.games_stats;

\set csv_path '\'' :folder_path '\\games_stats.csv\''
copy mbb.games_stats from :csv_path with header delimiter as ',' csv quote as '"';

commit;
