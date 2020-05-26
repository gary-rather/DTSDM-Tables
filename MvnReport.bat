set JAVA_HOME=D:\Java\jdk\jdk8
set MAVEN_HOME=X:\java\apps\maven\apache-maven-3.1.1
set PATH=%PATH%;%MAVEN_HOME%\bin;%JAVA_HOME%\bin

call mvn  clean

call mvn test
call mvn surefire-report:report 
pause