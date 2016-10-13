# Standard install path for infra 
%define install_path /usr/lib/
%define install_dir_name db-flume-agent-kafka-to-es
%define bin_name db-flume-agent-kafka-to-es
%define debug_package %{nil}
%define __jar_repack %{nil}
%define __arch_install_post %{nil}
%define __os_install_post %{nil}

Summary:	Flume agent to transfer log and audit data of databases from Kafka to ElasticSearch
Name:		cerndb-infra-db-flume-agent-kafka-to-es
Version:	0.0.1
Release:	1%{?dist}
License:	GPL
BuildArch:	noarch
Group:		Development/Tools
Source:		%{name}-%{version}.tar.gz
BuildRoot:	%{_builddir}/%{name}-root
AutoReqProv:	no

%description
Flume agent to transfer log and audit data of databases from Kafka to ElasticSearch

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

mkdir -p $RPM_BUILD_ROOT/etc/%{install_dir_name}/conf/
cp -a ./conf/* $RPM_BUILD_ROOT/etc/%{install_dir_name}/conf/
ln -sf /etc/%{install_dir_name}/conf $RPM_BUILD_ROOT/%{install_path}/%{install_dir_name}/conf

mkdir -p $RPM_BUILD_ROOT/var/lib/%{install_dir_name}/ 

mkdir -p $RPM_BUILD_ROOT/var/run/%{install_dir_name}/ 

mkdir -p $RPM_BUILD_ROOT/var/log/%{install_dir_name}/

# Install service
mkdir -p $RPM_BUILD_ROOT/etc/init.d/
ln -sf %{install_path}/%{install_dir_name}/bin/%{bin_name} $RPM_BUILD_ROOT/etc/init.d/%{bin_name}
mkdir -p $RPM_BUILD_ROOT/etc/rc0.d/
ln -sf /etc/init.d/%{bin_name} $RPM_BUILD_ROOT/etc/rc0.d/K99%{bin_name}
mkdir -p $RPM_BUILD_ROOT/etc/rc1.d/
ln -sf /etc/init.d/%{bin_name} $RPM_BUILD_ROOT/etc/rc1.d/K99%{bin_name}
mkdir -p $RPM_BUILD_ROOT/etc/rc2.d/
ln -sf /etc/init.d/%{bin_name} $RPM_BUILD_ROOT/etc/rc2.d/K99%{bin_name}
mkdir -p $RPM_BUILD_ROOT/etc/rc3.d/
ln -sf /etc/init.d/%{bin_name} $RPM_BUILD_ROOT/etc/rc3.d/S99%{bin_name}
mkdir -p $RPM_BUILD_ROOT/etc/rc4.d/
ln -sf /etc/init.d/%{bin_name} $RPM_BUILD_ROOT/etc/rc4.d/S99%{bin_name}
mkdir -p $RPM_BUILD_ROOT/etc/rc5.d/
ln -sf /etc/init.d/%{bin_name} $RPM_BUILD_ROOT/etc/rc5.d/S99%{bin_name}
mkdir -p $RPM_BUILD_ROOT/etc/rc6.d/
ln -sf /etc/init.d/%{bin_name} $RPM_BUILD_ROOT/etc/rc6.d/K99%{bin_name}


%clean
rm -rf ${RPM_BUILD_ROOT}

%files
%defattr(-,flume,flume,-)

%{install_path}/*

/etc/init.d/*
/etc/rc?.d/*

%attr(755, flume, flume) %{install_path}/%{install_dir_name}/bin/*

%dir /etc/%{install_dir_name}/conf/
%attr(755, flume, flume) /etc/%{install_dir_name}/conf/
%attr(644, flume, flume) /etc/%{install_dir_name}/conf/*

%dir /var/lib/%{install_dir_name}/

%attr(755, flume, flume) /var/lib/%{install_dir_name}/

%dir /var/run/%{install_dir_name}/
%attr(755, flume, flume) /var/run/%{install_dir_name}/

%dir /var/log/%{install_dir_name}/
%attr(755, flume, flume) /var/log/%{install_dir_name}/


%post
%{install_path}/%{install_dir_name}/bin/replace-old-flume-libraries
%{install_path}/%{install_dir_name}/bin/%{bin_name} stop
%{install_path}/%{install_dir_name}/bin/%{bin_name} start


# Please keep a meaningful changelog
%changelog
* Thu Oct 13 2016 Daniel Lanza <daniel.lanza@cern.ch> - 0.0.1-1
- Initial creation of the RPM.


