begin;

drop table if exists mbb.school_loc;

create table mbb.school_loc (
	season		int,
	school_id	int,
	arena_comb	text,
	city		text,
	latitude	float,
	longitude	float,
	primary key (season, school_id)
);

truncate table mbb.school_loc;

\set csv_path '\'' :folder_path '\\school_loc.csv\''
copy mbb.school_loc from :csv_path with header delimiter as ',' csv quote as '"';

commit;