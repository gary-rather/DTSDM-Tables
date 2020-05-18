set JAVA_HOME=D:\Java\jdk1.8.0_221
set MAVEN_HOME=D:\ApacheGroup\apache-maven-3.6.3
set PATH=%PATH%;%MAVEN_HOME%\bin;%JAVA_HOME%\bin

call mvn  clean
call mvn  test
pause