//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/tableref/subqueryref.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/statement/select_statement.hpp"
#include "parser/tableref/tableref.hpp"
#include "planner/bindcontext.hpp"

namespace duckdb {
//! Represents a subquery
class SubqueryRef : public TableRef {
  public:
	SubqueryRef(std::unique_ptr<SelectStatement> subquery);

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }

	//! The subquery
	std::unique_ptr<SelectStatement> subquery;
	// Bindcontext, FIXME
	std::unique_ptr<BindContext> context;
};
} // namespace duckdb
