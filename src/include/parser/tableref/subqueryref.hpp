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

#include "parser/statement/select_statement.hpp"
#include "parser/tableref/tableref.hpp"

namespace duckdb {
//! Represents a subquery
class SubqueryRef : public TableRef {
  public:
	SubqueryRef() : TableRef(TableReferenceType::SUBQUERY) {}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }

	std::unique_ptr<SelectStatement> subquery;
};
} // namespace duckdb
