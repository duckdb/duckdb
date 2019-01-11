//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/expression/star_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression.hpp"
#include "parser/sql_node_visitor.hpp"

namespace duckdb {

//! Represents a * expression in the SELECT clause
class StarExpression : public Expression {
public:
	StarExpression() : Expression(ExpressionType::STAR) {
	}

	ExpressionClass GetExpressionClass() override {
		return ExpressionClass::STAR;
	}

	unique_ptr<Expression> Copy() override;

	//! Deserializes a blob back into a StarExpression
	static unique_ptr<Expression> Deserialize(ExpressionType type, TypeId return_type, Deserializer &source);

	string ToString() const override {
		return "*";
	}
};
} // namespace duckdb
