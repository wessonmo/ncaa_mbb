begin;

drop table if exists ncaa.game_ids;

create table ncaa.game_ids (
	season_id	float,
	school_id	float,
	opp_id		float,
	game_id		float
);

truncate table ncaa.game_ids;

\set csv_path '\'' :folder_path '\\game_ids.csv\''
copy ncaa.game_ids from :csv_path with header delimiter as ',' csv quote as '"';

delete from ncaa.game_ids where game_id is null;

alter table ncaa.game_ids add primary key (school_id, game_id);

commit;