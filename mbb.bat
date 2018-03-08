echo off

if "%computername%"=="MITCHELL-LAPTOP" (
	set python=C:\Users\wesso\Anaconda2\python.exe
) else (
	set python=C:\Anaconda2\python.exe
)

REM %python% -W ignore %~dp0\scrapers\school_divs.py
REM %python% -W ignore %~dp0\scrapers\school_info_and_games.py
REM %python% -W ignore %~dp0\scrapers\game_hrefs.py
REM %python% -W ignore %~dp0\scrapers\starters.py

REM %python% -W ignore %~dp0\geo\game_and_school_locations.py
REM %python% -W ignore %~dp0\geo\school_distance.py

for /f %%a in ('psql -U postgres -c "select 1 as result from pg_database where datname='ncaa'" -t') do set /a check=%%a

if not defined check (createdb -U postgres ncaa)

psql -U postgres -d ncaa -qc "drop schema if exists mbb cascade; create schema mbb;"

psql -U postgres -d ncaa -v folder_path=%~dp0\csv -qf loaders/school_divs.sql
psql -U postgres -d ncaa -v folder_path=%~dp0\csv -qf loaders/school_info.sql
psql -U postgres -d ncaa -v folder_path=%~dp0\csv -qf loaders/games.sql
psql -U postgres -d ncaa -v folder_path=%~dp0\csv -qf loaders/school_loc.sql
psql -U postgres -d ncaa -v folder_path=%~dp0\csv -qf loaders/game_loc.sql
psql -U postgres -d ncaa -v folder_path=%~dp0\csv -qf loaders/school_dist.sql

pause