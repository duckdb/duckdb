<pre>
======================================================================
                 __________  ____  ____  _________
                / ____/ __ \/ __ \/ __ \/ ____/   |
               / / __/ /_/ / / / / /_/ / /   / /| |
              / /_/ / ____/ /_/ / _, _/ /___/ ___ |
              \____/_/    \____/_/ |_|\____/_/  |_|
                  The Greenplum Query Optimizer
              Copyright (c) 2015, Pivotal Software, Inc.
            Licensed under the Apache License, Version 2.0
======================================================================
</pre>

Welcome to GPORCA, the Greenplum Next Generation Query Optimizer!

To understand the objectives and architecture of GPORCA please refer to the following articles:
* [Orca: A Modular Query Optimizer Architecture for Big Data](https://tanzu.vmware.com/content/white-papers/orca-a-modular-query-optimizer-architecture-for-big-data).
* [Profiling Query Compilation Time with GPORCA](http://engineering.pivotal.io/post/orca-profiling/)
* [Improving Constraints In ORCA](http://engineering.pivotal.io/post/making-orca-smarter/)

Want to [Contribute](CONTRIBUTING.md)?

Questions? Connect with Greenplum on [Slack](https://greenplum.slack.com).

GPORCA supports various build types: debug, release with debug info, release.
You'll need CMake 3.1 or higher to build GPORCA. Get it from cmake.org, or your
operating system's package manager.

Note: GPDB 6X and later contain their own copy of GPORCA, this version is for GPDB 5X and any other uses.

# First Time Setup

## Clone GPORCA

```
git clone https://github.com/greenplum-db/gporca.git
cd gporca
```

## Pre-Requisites

GPORCA uses the following library:
* GP-Xerces - Greenplum's patched version of Xerces-C 3.1.X

### Installing GP-Xerces

[GP-XERCES is available here](https://github.com/greenplum-db/gp-xerces). The GP-XERCES README
gives instructions for building and installing.

## Build and install GPORCA

ORCA is built with [CMake](https://cmake.org), so any build system supported by
CMake can be used. The team uses [Ninja](https://ninja-build.org) because it's
really really fast and convenient.

Go into `gporca` directory:

```
cmake -GNinja -H. -Bbuild
ninja install -C build
```

<a name="test"></a>
## Test GPORCA

To run all GPORCA tests, simply use the `ctest` command from the build directory
after build finishes.

```
ctest
```

Much like `make`, `ctest` has a -j option that allows running multiple tests in
parallel to save time. Using it is recommended for faster testing.

```
ctest -j8
```

By default, `ctest` does not print the output of failed tests. To print the
output of failed tests, use the `--output-on-failure` flag like so (this is
useful for debugging failed tests):

```
ctest -j8 --output-on-failure
```

To run only the previously failed ctests, use the `--rerun-failed` flag.
```
ctest -j8 --rerun-failed --output-on-failure
```

To run a specific individual test, use the `gporca_test` executable directly.

```
./server/gporca_test -U CAggTest
```

To run a specific minidump, for example for `../data/dxl/minidump/TVFRandom.mdp`:
```
./server/gporca_test -d ../data/dxl/minidump/TVFRandom.mdp
```

Note that some tests use assertions that are only enabled for DEBUG builds, so
DEBUG-mode tests tend to be more rigorous.

<a name="addtest"></a>
## Adding tests

Most of the regression tests come in the form of a "minidump" file.
A minidump is an XML file that contains all the input needed to plan a query,
including information about all tables, datatypes, and functions used, as well
as statistics. It also contains the resulting plan.

A new minidump can be created by running a query on a live GPDB server:

1. Run these in a psql session:

```
set client_min_messages='log';
set optimizer=on;
set optimizer_enumerate_plans=on;
set optimizer_minidump=always;
set optimizer_enable_constant_expression_evaluation=off;
```

2. Run the query in the same psql session. It will create a minidump file
   under the "minidumps" directory, in the master's data directory:

```
$ ls -l $MASTER_DATA_DIRECTORY/minidumps/
total 12
-rw------- 1 heikki heikki 10818 Jun 10 22:02 Minidump_20160610_220222_4_14.mdp
```

3. Run xmllint on the minidump to format it better, and copy it under the
   data/dxl/minidump directory:

```
xmllint --format $MASTER_DATA_DIRECTORY/minidumps/Minidump_20160610_220222_4_14.mdp > data/dxl/minidump/MyTest.mdp
```

4. Add it to the test suite, in server/src/unittest/gpopt/minidump/CICGTest.cpp

```
--- a/server/src/unittest/gpopt/minidump/CICGTest.cpp
+++ b/server/src/unittest/gpopt/minidump/CICGTest.cpp
@@ -217,6 +217,7 @@ const CHAR *rgszFileNames[] =
                "../data/dxl/minidump/EffectsOfJoinFilter.mdp",
                "../data/dxl/minidump/Join-IDF.mdp",
                "../data/dxl/minidump/CoerceToDomain.mdp",
+               "../data/dxl/minidump/Mytest.mdp",
                "../data/dxl/minidump/LeftOuter2InnerUnionAllAntiSemiJoin.mdp",
 #ifndef GPOS_DEBUG
                // TODO:  - Jul 14 2015; disabling it for debug build to reduce testing time
```

Alternatively, it could also be added to the proper test suite in `server/CMakeLists.txt` as follows:
```
--- a/server/CMakeLists.txt
+++ b/server/CMakeLists.txt
@@ -183,7 +183,8 @@ CPartTbl5Test:
 PartTbl-IsNullPredicate PartTbl-IsNotNullPredicate PartTbl-IndexOnDefPartOnly
 PartTbl-SubqueryOuterRef PartTbl-CSQ-PartKey PartTbl-CSQ-NonPartKey
 PartTbl-LeftOuterHashJoin-DPE-IsNull PartTbl-LeftOuterNLJoin-DPE-IsNull
-PartTbl-List-DPE-Varchar-Predicates PartTbl-List-DPE-Int-Predicates;
+PartTbl-List-DPE-Varchar-Predicates PartTbl-List-DPE-Int-Predicates
+Mytest;
```

<a name="updatetest"></a>
## Update tests

In some situations, a failing test does not necessarily imply that the fix is
wrong. Occasionally, existing tests need to be updated. There is now a script
that allows for users to quickly and easily update existing mdps. This script
takes in a logfile that it will use to update the mdps. This logfile can be
obtained from running ctest as shown below.

Existing minidumps can be updated by runing the following:


1. Run `ctest -j8`.

2. If there are failing tests, run
```
ctest -j8 --rerun-failed --output-on-failure | tee /tmp/failures.out
```

3. The output file can then be used with the `fix_mdps.py` script.
```
gporca/scripts/fix_mdps.py --logFile /tmp/failures.out
```
Note: This will overwrite existing mdp files. This is best used after
committing existing changes, so you can more easily see the diff.
Alternatively, you can use `gporca/scripts/fix_mdps.py --dryRun` to not change
mdp files

4. Ensure that all changes are valid and as expected.

## Concourse
GPORCA contains a series of pipeline and task files to run various sets of tests
on [concourse](http://concourse.ci/). You can learn more about deploying concourse with
[bosh at bosh.io](http://bosh.io/).

Our concourse currently runs the following sets of tests:
* build and ctest on centos6
* build and ctest on centos7
* build and ctest on ubuntu18

All configuration files for our concourse pipelines can be found in the `concourse/` 
directory.

Note: concourse jobs and pipelines for GPORCA are currently experimental and should not be considered
ready for use in production-level CI environments.

# Advanced Setup

## How to generate build files with different options

Here are a few build flavors (commands run from the ORCA checkout directory):

```
# debug build
cmake -GNinja -D CMAKE_BUILD_TYPE=Debug -H. -Bbuild.debug
```

```
# release build with debug info
cmake -GNinja -D CMAKE_BUILD_TYPE=RelWithDebInfo -H. -Bbuild.release
```

## Explicitly Specifying GP-Xerces For Build

### GP-XERCES

It is recommended to use the `--prefix` option to the Xerces-C configure script
to install GP-Xerces in a location other than the default under `/usr/local/`,
because you may have other software that depends on Xerces-C, and the changes
introduced in the GP-Xerces patch make it incompatible with the upstream
version. Installing in a non-default prefix allows you to have GP-Xerces
installed side-by-side with unpatched Xerces without incompatibilities.

You can point cmake at your patched GP-Xerces installation using the
`XERCES_INCLUDE_DIR` and `XERCES_LIBRARY` options like so:

However, to use the current build scripts in GPDB, Xerces with the gp_xerces
patch will need to be placed on the /usr path.

```
cmake -GNinja -D XERCES_INCLUDE_DIR=/opt/gp_xerces/include -D XERCES_LIBRARY=/opt/gp_xerces/lib/libxerces-c.so ..
```

Again, on Mac OS X, the library name will end with `.dylib` instead of `.so`.

## Cross-Compiling 32-bit or 64-bit libraries

### GP-XERCES
Unless you intend to cross-compile a 32 or 64-bit version of GP-Orca, you can ignore these
instructions. If you need to explicitly compile for the 32 or 64-bit version of
your architecture, you need to set the `CFLAGS` and `CXXFLAGS` environment
variables for the configure script like so (use `-m32` for 32-bit, `-m64` for
64-bit):

```
CFLAGS="-m32" CXXFLAGS="-m32" ../configure --prefix=/opt/gp_xerces_32
```

### GPORCA

For the most part you should not need to explicitly compile a 32-bit or 64-bit
version of the optimizer libraries. By default, a "native" version for your host
platform will be compiled. However, if you are on x86 and want to, for example,
build a 32-bit version of Optimizer libraries on a 64-bit machine, you can do
so as described below. Note that you will need a "multilib" C++ compiler that
supports the -m32/-m64 switches, and you may also need to install 32-bit ("i386")
versions of the C and C++ standard libraries for your OS. Finally, you will need
to build 32-bit or 64-bit versions of GP-Xerces as appropriate.

Toolchain files for building 32 or 64-bit x86 libraries are located in the cmake
directory. Here is an example of building for 32-bit x86:

```
cmake -GNinja -D CMAKE_TOOLCHAIN_FILE=../cmake/i386.toolchain.cmake ../
```

And for 64-bit x86:

```
cmake -GNinja -D CMAKE_TOOLCHAIN_FILE=../cmake/x86_64.toolchain.cmake ../
```

## How to debug the build

Show all command lines while building (for debugging purpose)

```
ninja -v -C build
```

### Extended Tests

Debug builds of GPORCA include a couple of "extended" tests for features like
fault-simulation and time-slicing that work by running the entire test suite
in combination with the feature being tested. These tests can take a long time
to run and are not enabled by default. To turn extended tests on, add the cmake
arguments `-D ENABLE_EXTENDED_TESTS=1`.

## Installation Details

GPORCA has four libraries:

1. libnaucrates --- has all DXL related classes, and statistics related classes
2. libgpopt     --- has all the code related to the optimization engine, meta-data accessor, logical / physical operators,
                    transformation rules, and translators (DXL to expression and vice versa).
3. libgpdbcost  --- cost model for GPDB.
4. libgpos	--- abstraction of memory allocation, scheduling, error handling, and testing framework.

By default, GPORCA will be installed under /usr/local. You can change this by
setting CMAKE_INSTALL_PREFIX when running cmake, for example:
```
cmake -GNinja -D CMAKE_INSTALL_PREFIX=/home/user/gporca -H. -Bbuild
```

By default, the header files are located in:
```
/usr/local/include/naucrates
/usr/local/include/gpdbcost
/usr/local/include/gpopt
/usr/local/include/gpos
```
the library is located at:

```
/usr/local/lib/libnaucrates.so*
/usr/local/lib/libgpdbcost.so*
/usr/local/lib/libgpopt.so*
/usr/local/lib/libgpos.so*
```

Build and install:
```
ninja install -C build
```

### Common Issues

Note that because Red Hat-based systems do not normally look for shared
libraries in `/usr/local/lib`, it is suggested to add `/usr/local/lib` to the
/etc/ld.so.conf and run `ldconfig` to rebuild the shared library cache if
developing on one of these Linux distributions.

## Cleanup

Remove the `cmake` files generated under `build` folder of `gporca` repo:

```
rm -fr build/*
```

Remove gporca header files and library, (assuming the default install prefix /usr/local)

```
rm -rf /usr/local/include/naucrates
rm -rf /usr/local/include/gpdbcost
rm -rf /usr/local/include/gpopt
rm -rf /usr/local/include/gpos
rm -rf /usr/local/lib/libnaucrates.so*
rm -rf /usr/local/lib/libgpdbcost.so*
rm -rf /usr/local/lib/libgpopt.so*
rm -rf /usr/local/lib/libgpos.so*
```

<a name="contribute"></a>
# How to Contribute


ORCA has a [style guide](StyleGuilde.md), try to follow the existing style in your contribution to be consistent.

[clang-format]: https://clang.llvm.org/docs/ClangFormat.html
A set of [clang-format]-based rules are enforced in CI. Your editor or IDE may automatically support it. When in doubt, check formatting locally before submitting your PR:

```
CLANG_FORMAT=clang-format scripts/fmt chk
```

For more information, head over to the [formatting README](README.format.md).

Please see the [CONTRIBUTING](CONTRIBUTING.md) file for details.

