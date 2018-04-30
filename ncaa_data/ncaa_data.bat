echo off
echo.

set env=C:\Users\wessonmo\Anaconda2\python.exe
set max_season=2012

python ncaa_scrape.py %max_season%

echo.
pause