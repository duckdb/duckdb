#!/bin/bash

set -e

FILES="build/release/extension/*/*.duckdb_extension"
EXTENSION_LIST=""
for f in $FILES
do
	ext=`basename $f .duckdb_extension`
	EXTENSION_LIST="${EXTENSION_LIST}-$ext"
done
mkdir -p testext
cd testext
cmake -DCMAKE_BUILD_TYPE=Release -DTEST_REMOTE_INSTALL="${EXTENSION_LIST}" ..
cmake --build .
cd ..

for f in $FILES
do
	ext=`basename $f .duckdb_extension`
	install_path=${ext}
	if [ "$1" = "local" ]
	then
		install_path=${f}
	fi
	echo ${install_path}
	testext/duckdb -c "INSTALL '${install_path}'"
	testext/duckdb -c "LOAD '${ext}'"
done
testext/test/unittest "*"
