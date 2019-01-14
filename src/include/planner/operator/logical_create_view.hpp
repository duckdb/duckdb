//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_create_view.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

class LogicalCreateView : public LogicalOperator {
public:
	LogicalCreateView(SchemaCatalogEntry *schema, unique_ptr<CreateViewInformation> info)
	    : LogicalOperator(LogicalOperatorType::CREATE_VIEW), schema(schema), info(move(info)) {
	}

	vector<string> GetNames() override {
		return {"Whatever"};
	}

	//! Schema to insert to
	SchemaCatalogEntry *schema;
	//! Create Table information
	unique_ptr<CreateViewInformation> info;

protected:
	void ResolveTypes() override {
		types.push_back(TypeId::BIGINT);
	}
};
} // namespace duckdb
