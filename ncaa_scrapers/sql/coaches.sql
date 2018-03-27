begin;

drop table if exists ncaa.coaches;

create table ncaa.coaches (
	season		int,
	school_id	int,
	"order"		int,
	coach_id	int,
	coach_name	text,
	games		float,
	alma_mater	text,
	grad_year	int,
	primary key (season, school_id, coach_id)
);

truncate table ncaa.coaches;

\set csv_path '\'' :folder_path '\\coaches.csv\''
copy ncaa.coaches from :csv_path with header delimiter as ',' csv quote as '"';

commit;