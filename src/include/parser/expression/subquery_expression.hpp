//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/expression/subquery_expression.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression/tableref_expression.hpp"
#include "parser/statement/select_statement.hpp"

namespace duckdb {
//! Represents a subquery
class SubqueryExpression : public TableRefExpression {
  public:
	SubqueryExpression() : TableRefExpression(TableReferenceType::SUBQUERY) {}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }

	std::unique_ptr<SelectStatement> subquery;
};
} // namespace duckdb
