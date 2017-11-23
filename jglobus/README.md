jglobus-example
===============

This is a simple example of using the jglobus library to interact with a GridFTP server. Specifically, it just performs a remote 'ls' and prints out the results.

Make sure CA certiticates for your GridFTP setup are trusted.

myproxy-logon -s MYPROXY_HOSTNAME -l USERNAME

Change MYPROXY_HOSTNAME in src/main/java/com/mycompany/app/App.java

mvn compile assembly:single
java -jar target/my-app-1.0-SNAPSHOT-jar-with-dependencies.jar
