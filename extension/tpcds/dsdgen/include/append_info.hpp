#pragma once

#include "duckdb/main/appender.hpp"
#include "duckdb/main/connection.hpp"

#include <memory>

namespace tpcds {

struct tpcds_table_def {
	const char *name;
	int fl_small;
	int fl_child;
};

#define CALL_CENTER 0
#define DBGEN_VERSION 24

struct tpcds_append_information {
	tpcds_append_information(duckdb::Connection connection, std::string schema_name, std::string table_name) : connection(connection), appender(connection, schema_name, table_name)
	     {
	}

	duckdb::Connection connection;
	duckdb::Appender appender;

	tpcds_table_def table_def;
};

} // namespace tpcds
