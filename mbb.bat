echo off

if "%computername%"=="MITCHELL-LAPTOP" (
	set python=C:\Users\wesso\Anaconda2\python.exe
) else (
	set python=C:\Anaconda2\python.exe
)

REM %python% -W ignore %~dp0\ncaa_scrapers\school_divisions.py
REM %python% -W ignore %~dp0\ncaa_scrapers\school_info_and_results.py
REM %python% -W ignore %~dp0\ncaa_scrapers\game_ids.py
REM %python% -W ignore %~dp0\ncaa_scrapers\box_scores.py

REM %python% -W ignore %~dp0\geo\school_locations.py
REM %python% -W ignore %~dp0\geo\neutral_locations.py
REM %python% -W ignore %~dp0\geo\distances.py

REM %python% -W ignore %~dp0\kaggle\tournament_games.py

for /f %%a in ('psql -U postgres -c "select 1 as result from pg_database where datname='ncaa_mbb'" -t') do set /a check=%%a

if not defined check (createdb -U postgres ncaa_mbb)

psql -U postgres -d ncaa_mbb -qc "drop schema if exists ncaa cascade; create schema ncaa;"

psql -U postgres -d ncaa_mbb -v folder_path=%~dp0\ncaa_scrapers\csv -qf ncaa_scrapers\sql\school_divs.sql
psql -U postgres -d ncaa_mbb -v folder_path=%~dp0\ncaa_scrapers\csv -qf ncaa_scrapers\sql\school_info.sql
psql -U postgres -d ncaa_mbb -v folder_path=%~dp0\ncaa_scrapers\csv -qf ncaa_scrapers\sql\game_ids.sql
psql -U postgres -d ncaa_mbb -v folder_path=%~dp0\ncaa_scrapers\csv -qf ncaa_scrapers\sql\results.sql
psql -U postgres -d ncaa_mbb -v folder_path=%~dp0\ncaa_scrapers\csv -qf ncaa_scrapers\sql\box_scores.sql

REM psql -U postgres -d ncaa_mbb -qc "drop schema if exists geo cascade; create schema geo;"

REM psql -U postgres -d ncaa_mbb -v folder_path=%~dp0\geo\csv -qf geo\sql\school_coord.sql
REM psql -U postgres -d ncaa_mbb -v folder_path=%~dp0\geo\csv -qf geo\sql\neutral_coord.sql
REM psql -U postgres -d ncaa_mbb -v folder_path=%~dp0\geo\csv -qf geo\sql\game_dist.sql

REM psql -U postgres -d ncaa -v folder_path=%~dp0\csv -qf loaders/tourn_games.sql
REM psql -U postgres -d ncaa -v folder_path=%~dp0\csv -qf loaders/games_stats.sql

pause