#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

namespace duckdb {

class LogicalCreateMatView : public LogicalCreateTable {
public:
	LogicalCreateMatView(SchemaCatalogEntry *schema, unique_ptr<BoundCreateTableInfo> info)
	    : LogicalCreateTable(schema, move(info)) {
		type = LogicalOperatorType::LOGICAL_CREATE_MATVIEW;
	}
};
} // namespace duckdb
