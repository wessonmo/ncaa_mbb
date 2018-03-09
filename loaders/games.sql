begin;

drop table if exists mbb.games;

create table mbb.games (
	school_id		float,
	season			float,
	opp_id			float,
	game_date		date,
	school_score	float,
	opp_score		float,
	location		text,
	site			text,
	ot				float,
	attend			float
);

truncate table mbb.games;

\set csv_path '\'' :folder_path '\\games.csv\''
copy mbb.games from :csv_path with header delimiter as ',' csv quote as '"';

delete from mbb.games where opp_id is null;

alter table mbb.games add primary key (school_id, opp_id, game_date);

commit;