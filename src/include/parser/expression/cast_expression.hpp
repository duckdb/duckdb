//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/expression/cast_expression.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression/abstract_expression.hpp"

namespace duckdb {
//! Represents a type cast from one type to another type
class CastExpression : public AbstractExpression {
  public:
	CastExpression(TypeId target, std::unique_ptr<AbstractExpression> child)
	    : AbstractExpression(ExpressionType::OPERATOR_CAST, target,
	                         std::move(child)) {}

	virtual void ResolveType() override {
		AbstractExpression::ResolveType();
		stats = children[0]->stats;
		if (!stats.FitsInType(return_type)) {
			return_type = stats.MinimalType();
		}
	}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }
};
} // namespace duckdb
