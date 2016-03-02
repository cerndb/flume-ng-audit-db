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

flume-ng agent \
	--classpath $HOME/target/flume-ng-customizations-0.0.1-SNAPSHOT.jar \
	-n audit_agent \
	-f $HOME/conf/target/agent.conf \
	-c $HOME/lib/elasticsearch-sink2-1.0.jar 