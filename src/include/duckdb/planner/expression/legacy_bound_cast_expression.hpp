//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/legacy_bound_cast_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression.hpp"

namespace duckdb {
class ClientContext;

//! The BoundCastExpression has been moved to a function expression.
//! This class only exists for backwards compatible serialization of casts.
class LegacyBoundCastExpression : public Expression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::LEGACY_BOUND_CAST;

public:
	LegacyBoundCastExpression(unique_ptr<Expression> child, LogicalType target_type, bool try_cast);

	//! The child expression that is being cast
	unique_ptr<Expression> child;
	//! Whether or not this is a try_cast
	bool try_cast;

public:
	string ToString() const override;

	bool Equals(const BaseExpression &other) const override;
	unique_ptr<Expression> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<Expression> Deserialize(Deserializer &deserializer);

	static unique_ptr<Expression> DeserializeLegacyExpression(ClientContext &context, unique_ptr<Expression> child,
	                                                          const LogicalType &target_type, bool try_cast);
};
} // namespace duckdb
