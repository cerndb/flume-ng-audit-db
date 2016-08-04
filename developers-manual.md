# Developers manual

This section contains all the information developers may require.

## Version control system

A Git repository is used for the project. 

Find it at: https://gitlab.cern.ch/db/cerndb-infra-flume-ng-audit-db/ 

### Branches on Git

Different branches are kept for different purposes.

* main: development of components.
* build-gateway: all files that are deployed on the machine where the gateway is installed.
* build-agents: all files that are deployed on machines where data base agent is installed. 
* gitbook: all the documentation, basically this GitBook. 

Any other branch is used for temporal development.

## Continuous integration

For continuous integration Gitlab CI is used. With every commit test are run.

For build-gateway and build-agents branches, a new RPM is built in Koji if RPM spec file is modified in the commit. Therefore, version or release should be increased in the spec file, otherwise it will not pass the deployment phase but fail.

### Prepare Gitlab runners

A few machines have been prepare for running CI tests. These machines need to install:

* Gitlab runner: https://gitlab.com/gitlab-org/gitlab-ci-multi-runner/tree/master#install-gitlab-runner
* Maven
* MySql server

## Monitor agent with JMX metrics

In the script which sets the environment variables of the agent (/etc/flume-ng/db-flume-agent/conf/flume-env.sh), the following line need to be added:

`
export JAVA_OPTS=$JAVA_OPTS"-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=7778 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"
`

Once added, agent needs to be restated (as root):

`
service db-flume-agent restart
`

To open the port temporally in order to monitor it remotely:

* machine: to be replaced with the machine where the agent is running
* user: to be replaced with an user that is able to ssh the machine

`
ssh -D 7700 <user>@<machine>.cern.ch
`

Finally using jconsole you can monitor the process with:

* machine: to be replaced with the machine where the agent is running
 
`
jconsole -J-DsocksProxyHost=localhost -J-DsocksProxyPort=7700 service:jmx:rmi:///jndi/rmi://<machine>.cern.ch:7778/jmxrmi
`

7700 is the proxy port, any other can be used. Same for 7778, where service will listen to.

For Mac OSX, JConsole is located at: /System/Library/Frameworks/JavaVM.framework/Versions/Current/Commands/jconsole




