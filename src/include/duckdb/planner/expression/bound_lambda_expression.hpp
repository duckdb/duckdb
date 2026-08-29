//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/bound_lambda_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression.hpp"

namespace duckdb {

class BoundLambdaExpression : public Expression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::BOUND_LAMBDA;

public:
	BoundLambdaExpression(ExpressionType type_p, LogicalType return_type_p, unique_ptr<Expression> lambda_expr_p,
	                      idx_t parameter_count_p);

public:
	const unique_ptr<Expression> &LambdaExpr() const {
		return lambda_expr;
	}
	unique_ptr<Expression> &LambdaExprMutable() {
		return lambda_expr;
	}
	const vector<unique_ptr<Expression>> &Captures() const {
		return captures;
	}
	vector<unique_ptr<Expression>> &CapturesMutable() {
		return captures;
	}
	idx_t ParameterCount() const {
		return parameter_count;
	}
	idx_t &ParameterCountMutable() {
		return parameter_count;
	}
	const vector<Identifier> &ParameterNames() const {
		return parameter_names;
	}
	void SetParameterNames(vector<Identifier> names) {
		parameter_names = std::move(names);
	}

	string ToString() const override;
	bool Equals(const BaseExpression &other) const override;
	unique_ptr<Expression> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<Expression> Deserialize(Deserializer &deserializer);

private:
	//! The lambda expression that we'll use in the expression executor during execution
	unique_ptr<Expression> lambda_expr;
	//! Non-lambda constants, column references, and outer lambda parameters that we need to pass
	//! into the execution chunk
	vector<unique_ptr<Expression>> captures;
	//! The number of lhs parameters of the lambda function
	idx_t parameter_count;
	//! The names of the lhs parameters, purely for display - they do not take part in Equals, because two
	//! lambdas that differ only in their parameter names are the same expression. Can be empty, in which case
	//! ToString falls back to printing only the body
	vector<Identifier> parameter_names;
};
} // namespace duckdb
