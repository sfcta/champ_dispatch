@echo off
SET DISPATCH_PY_LOC=Y:\champ\util\bin

set yac=0
call :countargs %*
:: echo %yac% arguments
if %yac% EQU 0 (
  echo USAGE: dispatch jobset_file machine1 [machine2 machine3 ...]
  echo If the environment variable MACHINES is set, that overrides.
  goto eof
)

set AVAILABLEMACHINES=bonecrusher
:: ------------------------------------------------------------
:: Do not edit below this line!

set DPMACH=%MACHINES%
if "%MACHINES%" == "" set MACHINES=%2 %3 %4 %5 %6 %7 %8 %9
if "%MACHINES%"=="       " set MACHINES=%AVAILABLEMACHINES%

:attempt
python %DISPATCH_PY_LOC%\dispatcher.py %1 %MACHINES%
if ERRORLEVEL 2 goto :error
if ERRORLEVEL 1 goto :timeout

goto :done

:timeout
echo Dispatcher timed out; trying again!
goto :attempt

:error
echo Job failed; press CTRL-C to quit, or
pause

:countargs
:: quick little thing to count the number of args to this batch script
set yca=%1
if defined yca set /a yac+=1&shift&goto countargs
goto :eof

:done
set MACHINES=%DPMACH%
set DPMACH=

:eof
