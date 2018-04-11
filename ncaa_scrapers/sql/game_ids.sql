begin;

drop table if exists ncaa.game_ids;

create table ncaa.game_ids (
	season_id	float,
	school_id	float,
	game_date	date,
	opp_id		float,
	game_id		float
);

truncate table ncaa.game_ids;

\set csv_path '\'' :folder_path '\\game_ids_' :yr '.csv\''
copy ncaa.game_ids from :csv_path with header delimiter as ',' csv quote as '"';

delete from ncaa.game_ids where game_id is null;

commit;