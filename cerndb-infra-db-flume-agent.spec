# Standard install path for infra 
%define install_path /usr/lib/
%define install_dir_name db-flume-agent
%define debug_package %{nil}
%define __jar_repack %{nil}
%define __arch_install_post %{nil}
%define __os_install_post %{nil}

Summary:	Flume agent for gathering audit and log data from databases
Name:		cerndb-infra-db-flume-agent
Version:	0.1.4
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
cp -a ./LICENSE $RPM_BUILD_ROOT/%{install_path}/%{install_dir_name}/

mkdir -p $RPM_BUILD_ROOT/%{install_path}/%{install_dir_name}/bin/
cp -a ./bin/* $RPM_BUILD_ROOT/%{install_path}/%{install_dir_name}/bin/
sed -i 's/rpm_version/%{name}-%{version}-%{release}/g' $RPM_BUILD_ROOT/%{install_path}/%{install_dir_name}/bin/version

mkdir -p $RPM_BUILD_ROOT/%{install_path}/%{install_dir_name}/lib/
cp -a ./lib/* $RPM_BUILD_ROOT/%{install_path}/%{install_dir_name}/lib/

mkdir -p $RPM_BUILD_ROOT/etc/flume-ng/%{install_dir_name}/conf/
cp -a ./conf/* $RPM_BUILD_ROOT/etc/flume-ng/%{install_dir_name}/conf/
sed -i 's/rpm_version/%{version}-%{release}/g' $RPM_BUILD_ROOT/etc/flume-ng/%{install_dir_name}/conf/agent.conf.oracle-service.template
ln -sf /etc/flume-ng/%{install_dir_name}/conf $RPM_BUILD_ROOT/%{install_path}/%{install_dir_name}/conf

mkdir -p $RPM_BUILD_ROOT/var/lib/%{install_dir_name}/ 

mkdir -p $RPM_BUILD_ROOT/var/run/%{install_dir_name}/ 

mkdir -p $RPM_BUILD_ROOT/var/log/%{install_dir_name}/

# Install service
mkdir -p $RPM_BUILD_ROOT/etc/init.d/
ln -sf %{install_path}/%{install_dir_name}/bin/db-flume-agent $RPM_BUILD_ROOT/etc/init.d/db-flume-agent
mkdir -p $RPM_BUILD_ROOT/etc/rc0.d/
ln -sf /etc/init.d/db-flume-agent $RPM_BUILD_ROOT/etc/rc0.d/K99db-flume-agent
mkdir -p $RPM_BUILD_ROOT/etc/rc1.d/
ln -sf /etc/init.d/db-flume-agent $RPM_BUILD_ROOT/etc/rc1.d/K99db-flume-agent
mkdir -p $RPM_BUILD_ROOT/etc/rc2.d/
ln -sf /etc/init.d/db-flume-agent $RPM_BUILD_ROOT/etc/rc2.d/K99db-flume-agent
mkdir -p $RPM_BUILD_ROOT/etc/rc3.d/
ln -sf /etc/init.d/db-flume-agent $RPM_BUILD_ROOT/etc/rc3.d/S99db-flume-agent
mkdir -p $RPM_BUILD_ROOT/etc/rc4.d/
ln -sf /etc/init.d/db-flume-agent $RPM_BUILD_ROOT/etc/rc4.d/S99db-flume-agent
mkdir -p $RPM_BUILD_ROOT/etc/rc5.d/
ln -sf /etc/init.d/db-flume-agent $RPM_BUILD_ROOT/etc/rc5.d/S99db-flume-agent
mkdir -p $RPM_BUILD_ROOT/etc/rc6.d/
ln -sf /etc/init.d/db-flume-agent $RPM_BUILD_ROOT/etc/rc6.d/K99db-flume-agent


%clean
rm -rf ${RPM_BUILD_ROOT}

%files
%defattr(-,flume,flume,-)

%{install_path}/*

/etc/init.d/*
/etc/rc?.d/*

%attr(755, flume, flume) %{install_path}/%{install_dir_name}/bin/*

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
%{install_path}/%{install_dir_name}/bin/db-flume-agent stop
%{install_path}/%{install_dir_name}/bin/db-flume-agent start

# Please keep a meaningful changelog
%changelog
* Thu Oct 3 2016 Daniel Lanza <daniel.lanza@cern.ch> - 0.1.4-2
- Send agent version into Flume events

* Thu Sep 29 2016 Daniel Lanza <daniel.lanza@cern.ch> - 0.1.4-1
- Solve issue with null values in JSON documents

* Tue Sep 13 2016 Daniel Lanza <daniel.lanza@cern.ch> - 0.1.2-5
- Build connection URL with hostname instead of localhost

* Fri Aug 5 2016 Daniel Lanza <daniel.lanza@cern.ch> - 0.1.2-4
- Change gateway port to 10440

* Fri Jul 27 2016 Daniel Lanza <daniel.lanza@cern.ch> - 0.1.2-3
- Change logic for collecting metrics and reduce agent memory consumption

* Fri Jul 22 2016 Daniel Lanza <daniel.lanza@cern.ch> - 0.1.2-2
- Update JAR, differentiate between version 11 and 12 for audit and collect all metrics

* Mon Jul 18 2016 Daniel Lanza <daniel.lanza@cern.ch> - 0.1.2-1
- Collect database version

* Thu Jun 23 2016 Daniel Lanza <daniel.lanza@cern.ch> - 0.1.1-4
- Add ojdbc driver and get Java from /usr/bin/java

* Thu Jun 23 2016 Daniel Lanza <daniel.lanza@cern.ch> - 0.1.1-3
- Change directory structure and create Linux service, 

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


