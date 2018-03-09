begin;

drop table if exists mbb.tourn_games;

create table mbb.tourn_games (
	game_date	date,
	school_id	float,
	opp_id		float,
	tourn		int,
	primary key (game_date, school_id)
);

truncate table mbb.tourn_games;

\set csv_path '\'' :folder_path '\\tourn_games.csv\''
copy mbb.tourn_games from :csv_path with header delimiter as ',' csv quote as '"';

commit;