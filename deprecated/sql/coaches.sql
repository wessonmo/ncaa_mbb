begin;

drop table if exists ncaa.coaches;

create table ncaa.coaches (
	season		int,
	school_id	int,
	"order"		int,
	coach_id	float,
	coach_name	text,
	games		float,
	alma_mater	text,
	grad_year	float,
	primary key (season, school_id, coach_name)
);

truncate table ncaa.coaches;

\set csv_path '\'' :folder_path '\\coaches.csv\''
copy ncaa.coaches from :csv_path with header delimiter as ',' csv quote as '"';

commit;