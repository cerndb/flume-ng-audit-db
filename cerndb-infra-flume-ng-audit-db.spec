# Standard install path for infra stuff
%define install_path /ORA/dbs01/syscontrol/projects
%define debug_package %{nil}
%define __jar_repack %{nil}
%define __arch_install_post %{nil}
%define __os_install_post %{nil}

Summary:	Flume customisations for gathering audit ang log data from databases
Name:		cerndb-infra-flume-ng-audit-db
Version:	0.0.3
Release:	1%{?dist}
License:	GPL
BuildArch:	noarch
Group:		Development/Tools
Source:		%{name}-%{version}.tar.gz
BuildRoot:	%{_builddir}/%{name}-root
AutoReqProv:	no

%description
Flume customisations for gathering audit data and log from databases

%prep
%setup -q

%build

%install
%{__rm} -rf %{buildroot}
mkdir -p $RPM_BUILD_ROOT/var/log/flume-ng-audit-db/
mkdir -p $RPM_BUILD_ROOT/var/lib/flume-ng-audit-db/ 
mkdir -p $RPM_BUILD_ROOT/%{install_path}/flume-ng-audit-db/
cp -a ./dist/* $RPM_BUILD_ROOT/%{install_path}/flume-ng-audit-db/

%clean
rm -rf ${RPM_BUILD_ROOT}

%files
%defattr(-,root,root,-)
%{install_path}/*
%dir /var/log/flume-ng-audit-db/
%attr(755, oracle, ci) /var/log/flume-ng-audit-db/
%dir /var/lib/flume-ng-audit-db/
%attr(755, oracle, ci) /var/lib/flume-ng-audit-db/

# Please keep a meaningful changelog
%changelog
* Tue May 10 2016 Daniel Lanza <daniel.lanza@cern.ch> - 0.0.2-1
- Initial creation of the RPM.
* Tue May 11 2016 Daniel Lanza <daniel.lanza@cern.ch> - 0.0.2-2
- Installation directory properly configured
