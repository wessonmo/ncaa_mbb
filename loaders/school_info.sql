begin;

drop table if exists mbb.school_info;

create table mbb.school_info (
	school_id	int,
	season		int,
	city		text,
	arena		text,
	capacity	int,
	primary key (school_id, season)
);

truncate table mbb.school_info;

\set csv_path '\'' :folder_path '\\school_info.csv\''
copy mbb.school_info from :csv_path with header delimiter as ',' csv quote as '"';

commit;