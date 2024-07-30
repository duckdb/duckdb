//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/bound_lambdaref_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

//! A BoundLambdaRef expression represents a LambdaRef expression that was bound to an lambda parameter
//! in the lambda bindings vector. When capturing lambdas, the BoundLambdaRef becomes a
//! BoundReferenceExpression, indexing the corresponding lambda parameter in the lambda bindings vector,
//! which refers to the physical chunk of the lambda parameter during execution.
class BoundLambdaRefExpression : public Expression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::BOUND_LAMBDA_REF;

public:
	BoundLambdaRefExpression(LogicalType type, ColumnBinding binding, idx_t lambda_idx, idx_t depth = 0);
	BoundLambdaRefExpression(string alias, LogicalType type, ColumnBinding binding, idx_t lambda_idx, idx_t depth = 0);
	//! Column index set by the binder, used to generate the final BoundExpression
	ColumnBinding binding;
	//! The index of the lambda parameter in the lambda bindings vector
	idx_t lambda_idx;
	//! The subquery depth (i.e. depth 0 = current query, depth 1 = parent query, depth 2 = parent of parent, etc...).
	//! This is only non-zero for correlated expressions inside subqueries.
	idx_t depth;

public:
	bool IsScalar() const override {
		return false;
	}
	bool IsFoldable() const override {
		return false;
	}

	string ToString() const override;

	bool Equals(const BaseExpression &other) const override;
	hash_t Hash() const override;

	unique_ptr<Expression> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<Expression> Deserialize(Deserializer &deserializer);
};
} // namespace duckdb
