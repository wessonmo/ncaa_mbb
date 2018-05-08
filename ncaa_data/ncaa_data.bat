echo off
echo.

set env=C:\Users\wessonmo\Anaconda2\python.exe
set max_season=2014

python ncaa_data.py %max_season%

echo.
pause