//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/bound_unnest_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "duckdb/planner/expression.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {
class BaseExpression;
class Deserializer;
class Serializer;

//! Represents a function call that has been bound to a base function
class BoundUnnestExpression : public Expression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::BOUND_UNNEST;

public:
	explicit BoundUnnestExpression(LogicalType return_type);

	unique_ptr<Expression> child;

public:
	bool IsFoldable() const override;
	string ToString() const override;

	hash_t Hash() const override;
	bool Equals(const BaseExpression &other) const override;

	unique_ptr<Expression> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<Expression> Deserialize(Deserializer &deserializer);
};
} // namespace duckdb
