//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// planner/operator/logical_create.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

class LogicalCreate : public LogicalOperator {
  public:
	LogicalCreate(SchemaCatalogEntry *schema,
	              std::unique_ptr<CreateTableInformation> info)
	    : LogicalOperator(LogicalOperatorType::CREATE), schema(schema),
	      info(move(info)) {
	}

	void Accept(LogicalOperatorVisitor *v) override {
		v->Visit(*this);
	}

	//! Schema to insert to
	SchemaCatalogEntry *schema;
	//! Create Table information
	std::unique_ptr<CreateTableInformation> info;
};
} // namespace duckdb
