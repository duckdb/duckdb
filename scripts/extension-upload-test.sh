#!/bin/bash

set -e
set -x

CMAKE_CONFIG=Release
EXT_BASE_PATH=build/release

if [ "${FORCE_32_BIT:0}" == "1" ]; then
  FORCE_32_BIT_FLAG="-DFORCE_32_BIT=1"
else
  FORCE_32_BIT_FLAG=""
fi

FILES="${EXT_BASE_PATH}/extension/*/*.duckdb_extension"
EXTENSION_LIST=""
for f in $FILES
do
	ext=`basename $f .duckdb_extension`
	EXTENSION_LIST="${EXTENSION_LIST}-$ext"
done
mkdir -p testext
cd testext
cmake -DCMAKE_BUILD_TYPE=${CMAKE_CONFIG} ${FORCE_32_BIT_FLAG} -DTEST_REMOTE_INSTALL="${EXTENSION_LIST}" ..
cmake --build . --config ${CMAKE_CONFIG}
cd ..

duckdb_path="testext/duckdb"
unittest_path="testext/test/unittest"
if [ ! -f "${duckdb_path}" ]; then
	duckdb_path="testext/${CMAKE_CONFIG}/duckdb.exe"
	unittest_path="testext/test/${CMAKE_CONFIG}/unittest.exe"
fi

for f in $FILES
do
	ext=`basename $f .duckdb_extension`
	install_path=${ext}
	unsigned_flag=
	if [ "$1" = "local" ]
	then
		install_path=${f}
		unsigned_flag=-unsigned
	fi
	echo ${install_path}
	${duckdb_path} ${unsigned_flag} -c "INSTALL '${install_path}'"
	${duckdb_path} ${unsigned_flag} -c "LOAD '${ext}'"
done
${unittest_path}
