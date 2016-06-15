# If this file is placed at FLUME_CONF_DIR/flume-env.sh, it will be sourced
# during Flume startup.

# Enviroment variables can be set here.

JAVA_OPTS="-Xms1024m -Xmx2048m"

# Limit vmem usage
# https://issues.apache.org/jira/browse/HADOOP-7154
export MALLOC_ARENA_MAX=4     