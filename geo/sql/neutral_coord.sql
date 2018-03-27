begin;

drop table if exists geo.neutral_coord;

create table geo.neutral_coord (
	site		text,
	latitude	float,
	longitude	float,
	primary key (site)
);

truncate table geo.neutral_coord;

\set csv_path '\'' :folder_path '\\neutral_coord.csv\''
copy geo.neutral_coord from :csv_path with header delimiter as ',' csv quote as '"';

commit;