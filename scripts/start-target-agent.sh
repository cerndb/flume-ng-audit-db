flume-ng agent \
--classpath ../target/flume-ng-customizations-0.0.1-SNAPSHOT.jar \
-n audit_agent \
-c ../../lib/ElasticsearchSink2/build/libs/elasticsearch-sink2-assembly-1.0.jar \
-f ../conf/target/agent.conf 