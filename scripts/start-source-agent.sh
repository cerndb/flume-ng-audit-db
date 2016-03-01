/usr/lib/aimon-flume-ng/bin/flume-ng agent \
--classpath $ORACLE_HOME/jdbc/lib/ojdbc8.jar:../src/main/java/ \
-n audit_agent \
-c ../conf/source/ \
-f ../conf/source/agent.conf  