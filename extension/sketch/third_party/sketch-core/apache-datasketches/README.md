# Apache DataSketches Core C++ Library Component
This is the core C++ component of the Apache DataSketches library.  It contains all of the key sketching algorithms that are in the Java component and can be accessed directly from user applications. 

This component is also a dependency of other components of the library that create adaptors for target systems, such as PostgreSQL.

Note that we have a parallel core component for Java implementations of the same sketch algorithms, 
[datasketches-java](https://github.com/apache/datasketches-java).

Please visit the main [Apache DataSketches website](https://datasketches.apache.org) for more information. 

If you are interested in making contributions to this site please see our [Community](https://datasketches.apache.org/docs/Community/) page for how to contact us.

---

This code requires C++11.

This includes Python bindings. For the Python interface, see the README notes in [the python subdirectory](https://github.com/apache/datasketches-cpp/tree/master/python).

This library is header-only. The build process provided is only for building unit tests and the python library.

Building the unit tests requires cmake 3.12.0 or higher.

Installing the latest cmake on OSX: brew install cmake

Building and running unit tests using cmake for OSX and Linux:

```
    $ cmake -S . -B build/Release -DCMAKE_BUILD_TYPE=Release
    $ cmake --build build/Release -t all test
```

Building and running unit tests using cmake for Windows from the command line:

```
    $ cd build
    $ cmake ..
    $ cd ..
    $ cmake --build build --config Release
    $ cmake --build build --config Release --target RUN_TESTS
```

To install a local distribution (OSX and Linux), use the following command. The
CMAKE_INSTALL_PREFIX variable controls the destination. If not specified, it 
defaults to installing in /usr (/usr/include, /usr/lib, etc). In the command below,
the installation will be in /tmp/install/DataSketches (/tmp/install/DataSketches/include,
/tmp/install/DataSketches/lib, etc)

```
    $ cmake -S . -B build/Release -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/tmp/install/DataSketches
    $ cmake --build build/Release -t install
```

To generate an installable package using cmake's built in cpack packaging tool,
use the following command. The type of packaging is controlled by the CPACK_GENERATOR
variable (semi-colon separated list). Cmake usually supports packaging types such as RPM,
DEB, STGZ, TGZ, TZ, ZIP, etc.

```
    $ cmake3 -S . -B build/Release -DCMAKE_BUILD_TYPE=Release -DCPACK_GENERATOR="RPM;STGZ;TGZ" 
    $ cmake3 --build build/Release -t package
```

The DataSketches project can be included in other projects' CMakeLists.txt files in one of two ways.
If DataSketches has been installed on the host (using an RPM, DEB, "make install" into /usr/local, or some 
way, then CMake's `find_package` command can be used like this:

```
    find_package(DataSketches 3.2 REQUIRED)
    target_link_library(my_dependent_target PUBLIC ${DATASKETCHES_LIB})
```

When used with find_package, DataSketches exports several variables, including

   - `DATASKETCHES_VERSION`: The version number of the datasketches package that was imported.
   - `DATASKETCHES_INCLUDE_DIR`: The directory that should be added to access DataSketches include files.
   Because cmake automatically includes the interface directories for included target libraries when
   using `target_link_library`, under normal circumstances there will be no need to include this directly.
   - `DATASKETCHES_LIB`: The name of the DataSketches target to include as a dependency. Projects pulling
   in DataSketches should reference this with `target_link_library` in order to set up all the correct dependencies 
   and include paths.

If you don't have DataSketches installed locally, dependent projects can pull it directly
from GitHub using CMake's `ExternalProject` module. The code would look something like this:

```
    cmake_policy(SET CMP0097 NEW)
    include(ExternalProject)
    ExternalProject_Add(datasketches
        GIT_REPOSITORY https://github.com/apache/datasketches-cpp.git
        GIT_TAG 3.2.0
        GIT_SHALLOW true
        GIT_SUBMODULES ""
        INSTALL_DIR /tmp/datasketches-prefix
        CMAKE_ARGS -DBUILD_TESTS=OFF -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DCMAKE_INSTALL_PREFIX=/tmp/datasketches-prefix

        # Override the install command to add DESTDIR
        # This is necessary to work around an oddity in the RPM (but not other) package
        # generation, as CMake otherwise picks up the Datasketch files when building
        # an RPM for a dependent package. (RPM scans the directory for files in addition to installing
        # those files referenced in an "install" rule in the cmake file)
        INSTALL_COMMAND env DESTDIR= ${CMAKE_COMMAND} --build . --target install
    )
    ExternalProject_Get_property(datasketches INSTALL_DIR)
    set(datasketches_INSTALL_DIR ${INSTALL_DIR})
    message("Source dir of datasketches = ${datasketches_INSTALL_DIR}")
    target_include_directories(my_dependent_target 
                                PRIVATE ${datasketches_INSTALL_DIR}/include/DataSketches)
    add_dependencies(my_dependent_target datasketches)
```