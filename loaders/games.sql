begin;

drop table if exists mbb.games;

create table mbb.games (
	school_id	int,
	season		int,
	opp_id		int,
	game_date	date,
	team_score	int,
	opp_score	int,
	location	text,
	site		text,
	ot			int,
	attend		int,
	primary key (school_id, opp_id, game_date)
);

truncate table mbb.games;

\set csv_path '\'' :folder_path '\\games.csv\''
copy mbb.games from :csv_path with header delimiter as ',' csv quote as '"';

commit;