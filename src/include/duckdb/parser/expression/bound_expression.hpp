//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/bound_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

//! BoundExpression is an intermediate dummy class used by the binder. It is a ParsedExpression but holds an Expression.
//! It represents a successfully bound expression. It is used in the Binder to prevent re-binding of already bound parts
//! when dealing with subqueries.
class BoundExpression : public ParsedExpression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::BOUND_EXPRESSION;

public:
	explicit BoundExpression(unique_ptr<Expression> expr);

	unique_ptr<Expression> expr;

public:
	static unique_ptr<Expression> &GetExpression(ParsedExpression &expr);

	string ToString() const override;

	bool Equals(const BaseExpression &other) const override;
	hash_t Hash() const override;

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(Serializer &serializer) const override;
};

} // namespace duckdb
