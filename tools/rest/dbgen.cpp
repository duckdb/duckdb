#include <chrono>
#include <cstdio>
#include <stdlib.h> /* atoi */

#include "duckdb.hpp"
#include "dbgen.hpp"

#include "duckdb/common/string_util.hpp"

using namespace std;
using namespace duckdb;

void print_help() {
	fprintf(stderr, "🦆 Usage: duckdb_dbgen\n");
	fprintf(stderr, "         --database=[file]    use given database file\n");
	fprintf(stderr, "         --scale_factor=[sf]  TPCH scale factor (default: 1)\n");
}

int main(int argc, char **argv) {
	DBConfig config;
	string dbfile;
	double scale_factor = 1;

	// parse config
	for (int arg_index = 1; arg_index < argc; ++arg_index) {
		string arg = argv[arg_index];
		if (arg == "--help") {
			print_help();
			exit(0);
		} else if (StringUtil::StartsWith(arg, "--database=")) {
			auto splits = StringUtil::Split(arg, '=');
			if (splits.size() != 2) {
				print_help();
				exit(1);
			}
			dbfile = string(splits[1]);
		} else if (StringUtil::StartsWith(arg, "--scale_factor=")) {
			auto splits = StringUtil::Split(arg, '=');
			if (splits.size() != 2) {
				print_help();
				exit(1);
			}
			scale_factor = atof(splits[1].c_str());
		} else {
			fprintf(stderr, "Error: unknown argument %s\n", arg.c_str());
			print_help();
			exit(1);
		}
	}

	if (dbfile.empty()) {
		fprintf(stderr, "Error: need database file to generate\n");
		print_help();
		exit(1);
	}

	{
		DuckDB duckdb(dbfile, &config);
		tpch::dbgen(scale_factor, duckdb);
	} // hack to checkpoint WAL
	{ DuckDB duckdb(dbfile, &config); }
	return 0;
}
