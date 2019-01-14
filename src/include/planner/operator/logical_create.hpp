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

class LogicalCreate : public LogicalOperator {
public:
	LogicalCreate(SchemaCatalogEntry *schema, unique_ptr<CreateTableInformation> info)
	    : LogicalOperator(LogicalOperatorType::CREATE), schema(schema), info(move(info)) {
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
