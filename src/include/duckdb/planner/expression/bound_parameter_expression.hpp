//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/bound_parameter_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_parameter_data.hpp"

namespace duckdb {

class BoundParameterExpression : public Expression {
public:
	explicit BoundParameterExpression(idx_t parameter_nr);

	idx_t parameter_nr;
	shared_ptr<BoundParameterData> parameter_data;

public:
	bool IsScalar() const override;
	bool HasParameter() const override;
	bool IsFoldable() const override;

	string ToString() const override;

	bool Equals(const BaseExpression *other) const override;
	hash_t Hash() const override;

	unique_ptr<Expression> Copy() override;
};

} // namespace duckdb
