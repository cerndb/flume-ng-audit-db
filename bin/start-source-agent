#!/bin/bash

# Reference: http://stackoverflow.com/questions/59895/can-a-bash-script-tell-what-directory-its-stored-in
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
done
SCRIPT_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

HOME=../$SCRIPT_DIR

FLUME_BIN_DIR=/usr/lib/aimon-flume-ng/bin/

$FLUME_BIN_DIR/flume-ng agent \
	--classpath $ORACLE_HOME/jdbc/lib/ojdbc8.jar:$HOME/src/main/java/ \
	-n audit_agent \
	-f $HOME/conf/source/agent.conf \
	-c $HOME/conf/source/