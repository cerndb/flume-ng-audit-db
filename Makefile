REPOURL=git+ssh://git@gitlab.cern.ch:7999
REPOPREFIX=/db
REPONAME=cerndb-infra-flume-ng-audit-db

srpm:
	rpmbuild -bs --define '_sourcedir $(PWD)' ${SPECFILE}

agent_rpm:
	SPECFILE=cerndb-infra-db-flume-agent.spec
	PKGVERSION=$(shell awk '/Version:/ { print $$2 }' ${SPECFILE})
	PKGRELEASE=$(shell awk '/Release:/ { print $$2 }' ${SPECFILE} | sed -e 's/\%{?dist}//')
	PKGNAME=$(shell awk '/Name:/ { print $$2 }' ${SPECFILE})
	PKGID=$(PKGNAME)-$(PKGVERSION)
	
	rpmbuild -ba --define '_sourcedir $(PWD)' ${SPECFILE}
	
gateway_rpm:
	SPECFILE=cerndb-infra-db-flume-agents-gateway.spec
	PKGVERSION=$(shell awk '/Version:/ { print $$2 }' ${SPECFILE})
	PKGRELEASE=$(shell awk '/Release:/ { print $$2 }' ${SPECFILE} | sed -e 's/\%{?dist}//')
	PKGNAME=$(shell awk '/Name:/ { print $$2 }' ${SPECFILE})
	PKGID=$(PKGNAME)-$(PKGVERSION)
	
	rpmbuild -ba --define '_sourcedir $(PWD)' ${SPECFILE}

scratch:
	koji build db6 --nowait --scratch  ${REPOURL}${REPOPREFIX}/${REPONAME}.git#master
	koji build db7 --nowait --scratch  ${REPOURL}${REPOPREFIX}/${REPONAME}.git#master

build:
	koji build db6 ${REPOURL}${REPOPREFIX}/${REPONAME}.git#master
	koji build db7 ${REPOURL}${REPOPREFIX}/${REPONAME}.git#master
	
tag-qa-agent:
	koji tag-build db6-qa $(PKGID)-$(PKGRELEASE).el6
	koji tag-build db7-qa $(PKGID)-$(PKGRELEASE).el7.cern

tag-stable:
	koji tag-build db6-stable $(PKGID)-$(PKGRELEASE).el6
	koji tag-build db7-stable $(PKGID)-$(PKGRELEASE).el7.cern	


