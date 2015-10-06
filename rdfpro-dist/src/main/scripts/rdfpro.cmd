@echo off
@setlocal
set ERROR_CODE=0

if not "%JAVA_HOME%" == "" goto java_home_defined
echo Error: JAVA_HOME not set. >&2
goto error
:java_home_defined

if exist "%JAVA_HOME%\bin\java.exe" goto java_home_correct
echo Error: JAVA_HOME set to invalid directory "%JAVA_HOME%". >&2
goto error
:java_home_correct

set "RDFPRO_HOME=%~dp0"
if exist "%RDFPRO_HOME%\rdfpro.cmd" goto rdfpro_home_correct
echo Error. Could not locate RDFpro installation directory (tried "%RDFPRO_HOME%"). >&2
goto error
:rdfpro_home_correct

REM set the following to true to enable ANSI escape sequences under Windows
set RDFPRO_ANSI_ENABLED=false

"%JAVA_HOME%\bin\java.exe" %JAVA_OPTS% -classpath "%RDFPRO_HOME%etc;%RDFPRO_HOME%lib\*" eu.fbk.rdfpro.tool.Main %*
if ERRORLEVEL 1 goto error
goto end

:error
set ERROR_CODE=1

:end
@endlocal & set ERROR_CODE=%ERROR_CODE%
exit /B %ERROR_CODE%
