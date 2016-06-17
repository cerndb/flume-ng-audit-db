# Standard install path for infra 
%define install_path /usr/lib/
%define install_dir_name db-flume-agent
%define debug_package %{nil}
%define __jar_repack %{nil}
%define __arch_install_post %{nil}
%define __os_install_post %{nil}

Summary:	Flume customisations for gathering audit ang log data from databases
Name:		cerndb-infra-flume-ng-audit-db
Version:	0.1.1
Release:	2%{?dist}
License:	GPL
BuildArch:	noarch
Group:		Development/Tools
Source:		%{name}-%{version}.tar.gz
BuildRoot:	%{_builddir}/%{name}-root
AutoReqProv:	no

%description
Flume agent for gathering audit and log data from databases

%prep
%setup -q

%build

%install
%{__rm} -rf %{buildroot}

mkdir -p $RPM_BUILD_ROOT/%{install_path}/%{install_dir_name}/
cp -a ./dist/db-agents/LICENSE $RPM_BUILD_ROOT/%{install_path}/%{install_dir_name}/

mkdir -p $RPM_BUILD_ROOT/%{install_path}/%{install_dir_name}/bin/
cp -a ./dist/db-agents/bin/* $RPM_BUILD_ROOT/%{install_path}/%{install_dir_name}/bin/

mkdir -p $RPM_BUILD_ROOT/%{install_path}/%{install_dir_name}/lib/
cp -a ./dist/db-agents/lib/* $RPM_BUILD_ROOT/%{install_path}/%{install_dir_name}/lib/

mkdir -p $RPM_BUILD_ROOT/etc/flume-ng/%{install_dir_name}/conf/
cp -a ./dist/db-agents/conf/* $RPM_BUILD_ROOT/etc/flume-ng/%{install_dir_name}/conf/
ln -sf /etc/flume-ng/%{install_dir_name}/conf $RPM_BUILD_ROOT/%{install_path}/%{install_dir_name}/conf

mkdir -p $RPM_BUILD_ROOT/var/lib/%{install_dir_name}/ 

mkdir -p $RPM_BUILD_ROOT/var/run/%{install_dir_name}/ 

mkdir -p $RPM_BUILD_ROOT/var/log/%{install_dir_name}/


%clean
rm -rf ${RPM_BUILD_ROOT}

%files
%defattr(-,flume,flume,-)

%{install_path}/*

%dir /etc/flume-ng/%{install_dir_name}/conf/
%attr(755, flume, flume) /etc/flume-ng/%{install_dir_name}/conf/
%attr(644, flume, flume) /etc/flume-ng/%{install_dir_name}/conf/*

%dir /var/lib/%{install_dir_name}/
%attr(755, flume, flume) /var/lib/%{install_dir_name}/

%dir /var/run/%{install_dir_name}/
%attr(755, flume, flume) /var/run/%{install_dir_name}/

%dir /var/log/%{install_dir_name}/
%attr(755, flume, flume) /var/log/%{install_dir_name}/


%post
%{install_path}/%{install_dir_name}/bin/generate_agent_conf

# Please keep a meaningful changelog
%changelog
* Mon Jun 14 2016 Daniel Lanza <daniel.lanza@cern.ch> - 0.1.1-1
- Change configuration for several data flows and solve important issue when parsing timestamps 

* Mon Jun 6 2016 Daniel Lanza <daniel.lanza@cern.ch> - 0.1.0-1
- Run generate_agent_conf, allow configuration of password by specifying a command and use MD5 for hashing

* Tue May 20 2016 Daniel Lanza <daniel.lanza@cern.ch> - 0.0.3-2
- Run generate_agent_conf after deployment

* Tue May 19 2016 Daniel Lanza <daniel.lanza@cern.ch> - 0.0.3-1
- New features added and bug fixes

* Tue May 11 2016 Daniel Lanza <daniel.lanza@cern.ch> - 0.0.2-2
- Installation directory properly configured

* Tue May 10 2016 Daniel Lanza <daniel.lanza@cern.ch> - 0.0.2-1
- Initial creation of the RPM.