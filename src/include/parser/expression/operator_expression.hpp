//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/expression/operator_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression.hpp"
#include "parser/sql_node_visitor.hpp"

namespace duckdb {
//! Represents a built-in operator expression
class OperatorExpression : public Expression {
public:
	OperatorExpression(ExpressionType type, TypeId type_id = TypeId::INVALID) : Expression(type, type_id) {
	}
	OperatorExpression(ExpressionType type, TypeId type_id, unique_ptr<Expression> left,
	                   unique_ptr<Expression> right = nullptr)
	    : Expression(type, type_id, std::move(left), std::move(right)) {
	}

	void ResolveType() override;

	unique_ptr<Expression> Accept(SQLNodeVisitor *v) override {
		return v->Visit(*this);
	}
	ExpressionClass GetExpressionClass() override {
		return ExpressionClass::OPERATOR;
	}

	unique_ptr<Expression> Copy() override;

	//! Deserializes a blob back into an OperatorExpression
	static unique_ptr<Expression> Deserialize(ExpressionDeserializeInfo *info, Deserializer &source);
};
} // namespace duckdb
