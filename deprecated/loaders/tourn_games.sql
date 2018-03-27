begin;

drop table if exists mbb.ncaa_tourn_games;

create table mbb.ncaa_tourn_games (
	game_date	date,
	low_id		float,
	high_id		float,
	tourn		int,
	primary key (game_date, low_id)
);

truncate table mbb.ncaa_tourn_games;

\set csv_path '\'' :folder_path '\\ncaa_tourn_games.csv\''
copy mbb.ncaa_tourn_games from :csv_path with header delimiter as ',' csv quote as '"';

create table mbb.conf_tourn_games (
	game_date	date,
	low_id		float,
	high_id		float,
	tourn		int,
	primary key (game_date, low_id)
);

truncate table mbb.conf_tourn_games;

\set csv_path '\'' :folder_path '\\conf_tourn_games.csv\''
copy mbb.conf_tourn_games from :csv_path with header delimiter as ',' csv quote as '"';

commit;