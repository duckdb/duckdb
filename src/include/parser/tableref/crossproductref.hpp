//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/expression/crossproduct_expression.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/sql_node_visitor.hpp"
#include "parser/tableref/tableref.hpp"

namespace duckdb {
//! Represents a cross product
class CrossProductRef : public TableRef {
  public:
	CrossProductRef() : TableRef(TableReferenceType::CROSS_PRODUCT) {}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }

	virtual std::unique_ptr<TableRef> Copy() override {
		auto copy = make_unique<CrossProductRef>();
		copy->left = left->Copy();
		copy->right = right->Copy();
		copy->alias = alias;
		return copy;
	}

	//! The left hand side of the cross product
	std::unique_ptr<TableRef> left;
	//! The right hand side of the cross product
	std::unique_ptr<TableRef> right;
};
} // namespace duckdb
