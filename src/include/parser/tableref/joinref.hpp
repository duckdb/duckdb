//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/expression/join_expression.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression.hpp"
#include "parser/sql_node_visitor.hpp"
#include "parser/tableref/tableref.hpp"

namespace duckdb {
//! Represents a JOIN between two expressions
class JoinRef : public TableRef {
  public:
	JoinRef() : TableRef(TableReferenceType::JOIN) {}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }

	//! The left hand side of the join
	std::unique_ptr<TableRef> left;
	//! The right hand side of the join
	std::unique_ptr<TableRef> right;
	//! The join condition
	std::unique_ptr<Expression> condition;
	//! The join type
	JoinType type;
};
} // namespace duckdb
