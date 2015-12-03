
Name:		prodview
Version:	0.6
Release:	1%{?dist}
Summary:	A simple monitoring page for CMS global pool

Group:		Applications/Internet
License:	ASL 2.0
URL:		https://github.com/bbockelm/prodview

BuildArch:	noarch 

#
# To generate a new tarball, run the following from the source directory:
# python setup.py sdist
# cp dist/prodview-*.tar.gz ~/rpmbuild/SOURCES
#

# Github source0
# https://github.com/bbockelm/prodview/archive/prodview-0.1.tar.gz
Source0:	https://github.com/bbockelm/prodview/archive/prodview-%{version}.tar.gz
BuildRoot:	%(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)

BuildRequires:	python-genshi
Requires:	python-genshi
Requires:	mod_wsgi
Requires:	httpd
Requires:       rrdtool-python

%description
%{summary}


%prep
%setup -q


%build
python setup.py build


%install
rm -rf %{buildroot}
python setup.py install -O1 --root=$RPM_BUILD_ROOT --record=INSTALLED_FILES
mkdir -p $RPM_BUILD_ROOT/var/www/prodview
mkdir -p $RPM_BUILD_ROOT/var/www/analysisview
mkdir -p $RPM_BUILD_ROOT/var/www/analysiscrab2view
mkdir -p $RPM_BUILD_ROOT/var/www/totalview
mkdir -p $RPM_BUILD_ROOT/var/www/factoryview
mkdir -p $RPM_BUILD_ROOT/var/www/scheddview

%clean
rm -rf $RPM_BUILD_ROOT


%files -f INSTALLED_FILES
%defattr(-,root,root)
%config(noreplace) %_sysconfdir/prodview.conf
%config(noreplace) %_sysconfdir/httpd/conf.d/prodview-httpd.conf
%verify(not group user) %config(noreplace) %_sysconfdir/cron.d/prodview.cron
%verify(not group user) %config(noreplace) %_sysconfdir/cron.d/analysisview.cron
%verify(not group user) %config(noreplace) %_sysconfdir/cron.d/analysiscrab2.cron
%verify(not group user) %config(noreplace) %_sysconfdir/cron.d/totalview.cron
%verify(not group user) %config(noreplace) %_sysconfdir/cron.d/utilization.cron
%attr(0755,apache,apache) %dir /var/www/prodview
%attr(0755,apache,apache) %dir /var/www/prodview
%attr(0755,apache,apache) %dir /var/www/analysisview
%attr(0755,apache,apache) %dir /var/www/analysiscrab2view
%attr(0755,apache,apache) %dir /var/www/totalview
%attr(0755,apache,apache) %dir /var/www/factoryview
%attr(0755,apache,apache) %dir /var/www/scheddview

%changelog
* Thu Dec 03 2015 Justas Balcas <justas.balcas@cern.ch>  - 0.6-1
- Added crab2 view 9 May
- Added total, schedd, factory view - 3 Aug
- Resource utilization - 31 Aug
- Pilot usage in total view per site - 3 Sep
- Bootstrap 3 - 10 Nov
* Tue Aug 04 2015 Brian Bockelman <bbockelm@cse.unl.edu> - 0.5-1
- Cleanup file leaks.

* Fri May  8 2015 Justas Balcas <justas.balcas@cern.ch>  - 0.4-1
- Added analysis view

* Fri Apr 10 2015 Brian Bockelman <bbockelm@cse.unl.edu> - 0.3-1
- Adding a few extra columns to plots.

* Mon Mar 16 2015 Brian Bockelman <bbockelm@cse.unl.edu> - 0.2-1
- Finished prototype.

* Sat Mar 14 2015 Brian Bockelman <bbockelm@cse.unl.edu> - 0.1-1
- Initial prototype of prodview.


