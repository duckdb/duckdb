#pragma once

#include "planner/parsed_data/bound_create_table_info.hpp"
#include "planner/logical_operator.hpp"

namespace duckdb {

class LogicalCreateMatView : public LogicalCreateTable {
public:
    LogicalCreateMatView(SchemaCatalogEntry *schema, unique_ptr<BoundCreateTableInfo> info)
	    : LogicalCreateTable(schema, move(info)) {
        type = LogicalOperatorType::LOGICAL_CREATE_MATVIEW;
	}
};
} // namespace duckdb
