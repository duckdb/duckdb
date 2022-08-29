#!/bin/bash
set -x
rm -rf build/unixodbc
mkdir -p build/unixodbc
cd build/unixodbc
mkdir sources build
curl http://www.unixodbc.org/unixODBC-2.3.11.tar.gz | tar xvz -C sources --strip-components 1
cd sources
./configure --prefix `cd ../build; pwd` --disable-debug --disable-dependency-tracking --enable-static --enable-gui=no $@
make -j install