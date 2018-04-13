begin;

drop table if exists ncaa.results;
create table ncaa.results (
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

truncate table ncaa.results;

\set csv_path '\'' :folder_path '\\results.csv\''
copy ncaa.results from :csv_path with header delimiter as ',' csv quote as '"';

delete from ncaa.results where opp_id is null;

alter table ncaa.results add primary key (school_id, opp_id, game_date);

commit;