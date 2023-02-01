#!/bin/bash
set -x
rm -rf build/unixodbc
mkdir -p build/unixodbc
cd build/unixodbc
mkdir sources build
wget http://www.unixodbc.org/unixODBC-2.3.11.tar.gz || wget http://github.com/lurcher/unixODBC/releases/download/2.3.11/unixODBC-2.3.11.tar.gz
tar -xf unixODBC-2.3.11.tar.gz -C sources --strip-components 1
cd sources
./configure --prefix `cd ../build; pwd` --disable-debug --disable-dependency-tracking --enable-static --enable-gui=no $@
make -j install