Summary: Transactions library for the Couchbase
Name: couchbase-transactions

# for pre-release versions, Release must have "0." prefix,
# otherwise first number have to be greater than zero
Version: 1.0.0
Release: 0.alpha.1%{?dist}

Vendor: Couchbase, Inc.
Packager: Couchbase SDK Team <support@couchbase.com>
License: ASL 2.0
BuildRequires: gcc, gcc-c++
BuildRequires: cmake >= 3.9
BuildRequires: pkgconfig(libevent) >= 2
BuildRequires: openssl-devel
BuildRequires: boost-devel
URL: https://docs.couchbase.com/c-sdk/3.0/project-docs/distributed-transactions-c-release-notes.html
Source: couchbase-transactions-%{version}.tar.gz

%description
This package provides the library and all necessary headers to implement
Couchbase distributed transactions in the C++ application.

%prep
%autosetup -p1
%cmake  -B . -S . \
    -DBUILD_DOC=OFF \
    -DBUILD_TESTS=OFF \
    -DBUILD_EXAMPLES=OFF

%build
%make_build

%install
%make_install

%check

%ldconfig_scriptlets

%files -n %{name}
%{_libdir}/libtransactions_cxx.so*
%{_includedir}/couchbase/*
%license LICENCE.md
