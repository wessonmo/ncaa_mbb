echo off
echo.

set env=C:\Users\wessonmo\Anaconda2\python.exe
set seasons=2018 2018
set divisions=1 1
set multi_proc_bool=1

python ncaa_data.py %seasons% %divisions% %multi_proc_bool%

echo.
pause