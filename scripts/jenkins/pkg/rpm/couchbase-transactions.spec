Summary: Transactions library for the Couchbase
Name: couchbase-transactions

# for pre-release versions, Release must have "0." prefix,
# otherwise first number have to be greater than zero
Version: 1.0.0
Release: 1%{?dist}

Vendor: Couchbase, Inc.
Packager: Couchbase SDK Team <support@couchbase.com>
License: ASL 2.0
BuildRequires: gcc, gcc-c++
BuildRequires: cmake3 >= 3.9
BuildRequires: pkgconfig(libevent) >= 2
BuildRequires: openssl-devel
BuildRequires: boost169-devel
BuildRequires: boost169-static
BuildRequires: boost169-system
BuildRequires: boost169-thread
BuildRequires: boost169-date-time
URL: https://docs.couchbase.com/c-sdk/3.0/project-docs/distributed-transactions-cpp-release-notes.html
Source: couchbase-transactions-%{version}.tar.gz

%description
This package provides the library and all necessary headers to implement
Couchbase distributed transactions in the C++ application.

%prep
%autosetup -p1
%cmake3  \
    -DBUILD_DOC=OFF \
    -DBUILD_TESTS=OFF \
    -DBUILD_EXAMPLES=OFF \
    -DSTATIC_BOOST=ON \
    -DBOOST_INCLUDEDIR=%{_includedir}/boost169 \
    -DBOOST_LIBRARYDIR=%{_libdir}/boost169 \
    .

%build
%make_build

%install
%make_install

%check

%ldconfig_scriptlets

%files -n %{name}
%{_libdir}/libtransactions_cxx.so*
%{_includedir}/couchbase/*
%license LICENSE.md
