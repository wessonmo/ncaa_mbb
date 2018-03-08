begin;

drop table if exists mbb.school_dist;

create table mbb.school_dist (
	game_date	date,
	school_id	text,
	opp_id		text,
	school_dist	float,
	opp_dist	float,
	primary key (game_date, school_id, opp_id)
);

truncate table mbb.school_dist;

\set csv_path '\'' :folder_path '\\school_dist.csv\''
copy mbb.school_dist from :csv_path with header delimiter as ',' csv quote as '"';

commit;