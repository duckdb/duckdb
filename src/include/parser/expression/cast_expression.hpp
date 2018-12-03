//===----------------------------------------------------------------------===// 
// 
//                         DuckDB 
// 
// parser/expression/cast_expression.hpp
// 
// 
// 
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression.hpp"
#include "parser/sql_node_visitor.hpp"

namespace duckdb {
//! Represents a type cast from one type to another type
class CastExpression : public Expression {
  public:
	CastExpression(TypeId target, std::unique_ptr<Expression> child)
	    : Expression(ExpressionType::OPERATOR_CAST, target, std::move(child)) {
	}

	void ResolveType() override {
		Expression::ResolveType();
		ExpressionStatistics::Cast(children[0]->stats, stats);
		if (!stats.FitsInType(return_type)) {
			return_type = stats.MinimalType();
		}
	}

	std::unique_ptr<Expression> Copy() override;

	//! Deserializes a blob back into an CastExpression
	static std::unique_ptr<Expression>
	Deserialize(ExpressionDeserializeInformation *info, Deserializer &source);

	std::string ToString() const override {
		return "CAST[" + TypeIdToString(return_type) + "](" +
		       children[0]->ToString() + ")";
	}

	std::unique_ptr<Expression> Accept(SQLNodeVisitor *v) override {
		return v->Visit(*this);
	}
	ExpressionClass GetExpressionClass() override {
		return ExpressionClass::CAST;
	}
};
} // namespace duckdb
