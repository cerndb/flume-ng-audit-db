# If this file is placed at FLUME_CONF_DIR/flume-env.sh, it will be sourced
# during Flume startup.

# Enviroment variables can be set here.

JAVA_OPTS="-Xms50m -Xmx100m"

FLUME_JAVA_LIBRARY_PATH=/ORA/dbs01/oracle/product/rdbms/lib

# Limit vmem usage
# https://issues.apache.org/jira/browse/HADOOP-7154
export MALLOC_ARENA_MAX=4     