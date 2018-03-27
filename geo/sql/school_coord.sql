begin;

drop table if exists geo.school_coord;

create table geo.school_coord (
	season		int,
	school_id	int,
	latitude	float,
	longitude	float,
	primary key (season, school_id)
);

truncate table geo.school_coord;

\set csv_path '\'' :folder_path '\\school_coord.csv\''
copy geo.school_coord from :csv_path with header delimiter as ',' csv quote as '"';

commit;