#pragma once

#include "duckdb/main/appender.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/client_context.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/catalog/catalog.hpp"
#endif

#include <memory>
#include <cassert>

namespace tpcds {

struct tpcds_table_def {
	const char *name;
	int fl_small;
	int fl_child;
	int first_column;
};

#define CALL_CENTER   0
#define DBGEN_VERSION 24

struct tpcds_append_information {
	tpcds_append_information(duckdb::ClientContext &context_p, duckdb::TableCatalogEntry *table)
	    : context(context_p), appender(context_p, *table) {
	}

	duckdb::ClientContext &context;
	duckdb::InternalAppender appender;

	tpcds_table_def table_def;

	bool IsNull();
};

} // namespace tpcds
