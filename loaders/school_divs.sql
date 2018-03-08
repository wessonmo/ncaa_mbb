begin;

drop table if exists mbb.school_divs;

create table mbb.school_divs (
	school_id	int,
	season_href	text,
	school_name	text,
	season		int,
	division	int,
	primary key (school_id, season)
);

truncate table mbb.school_divs;

\set csv_path '\'' :folder_path '\\school_divs.csv\''
copy mbb.school_divs from :csv_path with header delimiter as ',' csv quote as '"';

commit;