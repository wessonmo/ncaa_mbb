begin;

drop table if exists geo.game_dist;
create table geo.game_dist (
	game_date	date,
	school_id	float,
	opp_id		float,
	school_dist	float,
	opp_dist	float,
	primary key (game_date, school_id, opp_id)
);

truncate table geo.game_dist;

\set csv_path '\'' :folder_path '\\game_dist.csv\''
copy geo.game_dist from :csv_path with header delimiter as ',' csv quote as '"';

commit;