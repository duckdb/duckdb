//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/expression/bound_parameter_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/value.hpp"
#include "planner/expression.hpp"

namespace duckdb {

class BoundParameterExpression : public Expression {
public:
	BoundParameterExpression(index_t parameter_nr);

	SQLType sql_type;
	index_t parameter_nr;
	Value *value;

public:
	bool IsScalar() const override;
	bool HasParameter() const override;
	bool IsFoldable() const override;

	string ToString() const override;

	bool Equals(const BaseExpression *other) const override;
	uint64_t Hash() const override;

	unique_ptr<Expression> Copy() override;
};
} // namespace duckdb
