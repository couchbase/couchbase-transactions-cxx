# Couchbase C++ Transactions Client

## Getting the Source Code

This repo uses several git submodules. If you are fetching the repo for the first time by command line, the
`--recurse-submodules` option will init the submodules recursively as well:
```shell
git clone --recurse-submodules git@//github.com:couchbase/couchbase-transactions-cxx.git
```

However, if you fetched using a simple clone command (or another IDE or tool) **you must also perform** the following
command to recursively update and initialize the submodules:
```shell
git submodule update --init --recursive
```

### Dev Dependencies

The following dependencies must be installed before the project can be built. We recommend using OS specific utilities
such as `brew`, `apt-get`, and similar package management utilities (depending on your environment).
- **cmake >= 3.17.0+** (e.g., `brew install cmake`)
- **c++ compiler >= std_17** (e.g., `xcode-select --install`)
- **openssl >= 1.1+** (e.g., `brew install openssl`)

**IMPORTANT:** On macOS, the **OpenSSL** `brew` install command mentioned above is not sufficient to be able to build.
The easiest way to fix this is to add the `OPENSSL_ROOT_DIR` env variable to your exports (e.g., `.zshenv`). If this is
not sufficient, see the other tips mentioned when you run `brew info openssl`.
```shell
export OPENSSL_ROOT_DIR=/usr/local/opt/openssl@1.1/
```
or, if you prefer:
```shell
export PKG_CONFIG_PATH=/usr/local/opt/openssl@1.1/lib/pkgconfig
```

## Building
From the root of the repo:
```shell
mkdir build
cd build
cmake ..
make -j8
```

## Running Tests
Currently, all the tests are packaged into one executable, which will be placed in the build directory:

```shell
./client_tests
```

