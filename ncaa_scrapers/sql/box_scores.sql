begin;

drop table if exists ncaa.box_scores;
create table ncaa.box_scores (
	game_id			int,
	period			float,
	school_id		float,
	player_order	float,
	player_href		text,
	player_name		text,
	pos				text,
	min				float,
	FGM				float,
	FGA				float,
	FG3				float,
	FG3A			float,
	FT				float,
	FTA				float,
	Pts				float,
	ORB				float,
	DRB				float,
	TRB				float,
	AST				float,
	"TO"			float,
	STL				float,
	BLK				float,
	FL				float
);

truncate table ncaa.box_scores;

\set csv_path '\'' :folder_path '\\box_scores.csv\''
copy ncaa.box_scores from :csv_path with header delimiter as ',' csv quote as '"';

delete from ncaa.box_scores where school_id is null;

alter table ncaa.box_scores add primary key (game_id, period, school_id, player_order);

commit;