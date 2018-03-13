begin;

drop table if exists mbb.games_stats;

create table mbb.games_stats (
	game_date	date,
	school_id	float,
	opp_id		float,
	teff		float,
	efg			float,
	pta_min		float,
	astp		float,
	blkp		float,
	orbp		float,
	drbp		float,
	primary key (game_date, school_id, opp_id)
);

truncate table mbb.games_stats;

\set csv_path '\'' :folder_path '\\games_stats.csv\''
copy mbb.games_stats from :csv_path with header delimiter as ',' csv quote as '"';

commit;