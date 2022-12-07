#!/bin/sh

SCRIPT=$(realpath "$0")
SCRIPTPATH=$(dirname "$SCRIPT")

echo $SCRIPTPATH

rm -rf	$SCRIPTPATH/.eggs \
		$SCRIPTPATH/.pytest_cache \
		$SCRIPTPATH/build \
		$SCRIPTPATH/dist \
		$SCRIPTPATH/duckdb.egg-info \
		$SCRIPTPATH/duckdb.cpp \
		$SCRIPTPATH/duckdb.hpp \
		$SCRIPTPATH/parquet-extension.cpp \
		$SCRIPTPATH/parquet-extension.hpp \
		$SCRIPTPATH/duckdb duckdb_tarball

rm -f	$SCRIPTPATH/sources.list \
		$SCRIPTPATH/includes.list \
		$SCRIPTPATH/githash.list

python3 -m pip uninstall duckdb --yes
