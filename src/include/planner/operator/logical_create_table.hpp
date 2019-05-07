//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_create_table.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_data/create_table_info.hpp"
#include "planner/logical_operator.hpp"

namespace duckdb {

class LogicalCreateTable : public LogicalOperator {
public:
	LogicalCreateTable(SchemaCatalogEntry *schema, unique_ptr<CreateTableInfo> info)
	    : LogicalOperator(LogicalOperatorType::CREATE_TABLE), schema(schema), info(move(info)) {
	}

	//! Schema to insert to
	SchemaCatalogEntry *schema;
	//! Create Table information
	unique_ptr<CreateTableInfo> info;

protected:
	void ResolveTypes() override {
		types.push_back(TypeId::BIGINT);
	}
};
} // namespace duckdb
