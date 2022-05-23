/// @file
/// @brief Base class providing schema information to grammar

#ifndef SQLSMITH_HH
#define SQLSMITH_HH

#include <cstdint>
#include "duckdb.hh"

namespace duckdb_sqlsmith {

struct SQLSmithOptions {
	int32_t seed = -1;
	uint64_t max_queries = 0;
	bool exclude_catalog = false;
	bool dump_all_queries = false;
	bool dump_all_graphs = false;
	bool verbose_output = false;
	std::string complete_log;
	std::string log;
};

int32_t run_sqlsmith(duckdb::DatabaseInstance &database, SQLSmithOptions options);

} // namespace duckdb_sqlsmith

#endif
