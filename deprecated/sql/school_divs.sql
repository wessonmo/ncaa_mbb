begin;

drop table if exists ncaa.school_divs;

create table ncaa.school_divs (
	season		int,
	season_id	int,
	school_id	int,
	school_name	text,
	division	int,
	primary key (season, school_id)
);

truncate table ncaa.school_divs;

\set csv_path '\'' :folder_path '\\school_divs.csv\''
copy ncaa.school_divs from :csv_path with header delimiter as ',' csv quote as '"';

commit;