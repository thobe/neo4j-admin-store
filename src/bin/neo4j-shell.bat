@echo off
SETLOCAL
set _SCRIPTS_=%~dp0

java %JAVA_OPTS% -cp ".;%_SCRIPTS_%\neo-${project.version}.jar:$SCRIPTDIR\geronimo-jta_${jta.version}_spec-1.1.1.jar;%_SCRIPTS_%\shell-${project.version}.jar;%_SCRIPTS_%\jline-${jline.version}.jar org.neo4j.shell.StartLocalClient %1 %2 %3 %4 %5 %6 %7 %8 %9