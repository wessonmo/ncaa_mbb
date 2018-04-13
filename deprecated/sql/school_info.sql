begin;

drop table if exists ncaa.school_info;

create table ncaa.school_info (
	school_id	int,
	season		int,
	city		text,
	arena		text,
	capacity	int,
	primary key (school_id, season)
);

truncate table ncaa.school_info;

\set csv_path '\'' :folder_path '\\school_info.csv\''
copy ncaa.school_info from :csv_path with header delimiter as ',' csv quote as '"';

commit;