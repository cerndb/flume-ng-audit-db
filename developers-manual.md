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