echo off
echo.

set db_name='ncaa_mbb2'

for /f %%a in ('psql -U postgres -c "select 1 as result from pg_database where datname=%db_name%" -t') do set /a check=%%a
if not defined check (createdb -U postgres ncaa_mbb2)

set env=C:\Users\wessonmo\Anaconda2\python.exe
set schema_name=ncaa_data
set storage_dir=E:\ncaa_mbb\ncaa_data
set max_season=2013

python new_ncaa_data.py %schema_name% %storage_dir% %max_season%

echo.
pause