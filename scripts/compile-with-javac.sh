/ORA/dbs01/oracle/product/rdbms/jdk/bin/javac \
-cp /ORA/dbs01/oracle/product/rdbms/jdbc/lib/ojdbc8.jar:/usr/lib/aimon-flume-ng/lib/*:/usr/lib/hadoop/* \
../src/main/java/ch/cern/db/audit/flume/*.java \
../src/main/java/ch/cern/db/audit/flume/sink/elasticsearch/serializer/*.java
../src/main/java/ch/cern/db/audit/flume/sink/kite/parser/*.java
../src/main/java/ch/cern/db/audit/flume/source/*.java \
../src/main/java/ch/cern/db/audit/flume/source/deserializer/*.java \
../src/main/java/ch/cern/db/audit/flume/source/reader/*.java \