echo off
echo.

set env=C:\Anaconda2\python.exe
set seasons=2015 2018
set divisions=1 1

python ncaa_data.py %seasons% %divisions%

echo.
pause