echo off
echo.

set db_name=ncaa_mbb

for /f %%a in ('psql -U postgres -c "select 1 as result from pg_database where datname='%db_name%'" -t') do set /a check=%%a
if not defined check (createdb -U postgres %db_name%)

set env=C:\Users\wessonmo\Anaconda2\python.exe
set schema_name=ncaa_data
set storage_dir=E:\ncaa_mbb\ncaa_data
set max_season=2018

python ncaa_data\get_ncaa_data.py %db_name% %schema_name% %storage_dir% %max_season%

echo.
pause
