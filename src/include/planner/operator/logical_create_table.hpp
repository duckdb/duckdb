//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_create.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

class LogicalCreateTable : public LogicalOperator {
public:
	LogicalCreateTable(SchemaCatalogEntry *schema, unique_ptr<CreateTableInformation> info)
	    : LogicalOperator(LogicalOperatorType::CREATE_TABLE), schema(schema), info(move(info)) {
	}

	vector<string> GetNames() override {
		return {"Count"};
	}

	//! Schema to insert to
	SchemaCatalogEntry *schema;
	//! Create Table information
	unique_ptr<CreateTableInformation> info;

protected:
	void ResolveTypes() override {
		types.push_back(TypeId::BIGINT);
	}
};
} // namespace duckdb
