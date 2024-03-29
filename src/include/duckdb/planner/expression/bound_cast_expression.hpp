//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/bound_cast_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression.hpp"
#include "duckdb/function/cast/default_casts.hpp"

namespace duckdb {

class BoundCastExpression : public Expression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::BOUND_CAST;

public:
	BoundCastExpression(unique_ptr<Expression> child, LogicalType target_type, BoundCastInfo bound_cast,
	                    bool try_cast = false);

	//! The child type
	unique_ptr<Expression> child;
	//! Whether to use try_cast or not. try_cast converts cast failures into NULLs instead of throwing an error.
	bool try_cast;
	//! The bound cast info
	BoundCastInfo bound_cast;

public:
	LogicalType source_type() { // NOLINT: allow casing for legacy reasons
		D_ASSERT(child->return_type.IsValid());
		return child->return_type;
	}

	//! Cast an expression to the specified SQL type, using only the built-in SQL casts
	static unique_ptr<Expression> AddDefaultCastToType(unique_ptr<Expression> expr, const LogicalType &target_type,
	                                                   bool try_cast = false);
	//! Cast an expression to the specified SQL type if required
	DUCKDB_API static unique_ptr<Expression> AddCastToType(ClientContext &context, unique_ptr<Expression> expr,
	                                                       const LogicalType &target_type, bool try_cast = false);

	//! If the expression returns an array, cast it to return a list with the same child type. Otherwise do nothing.
	DUCKDB_API static unique_ptr<Expression> AddArrayCastToList(ClientContext &context, unique_ptr<Expression> expr);

	//! Returns true if a cast is invertible (i.e. CAST(s -> t -> s) = s for all values of s). This is not true for e.g.
	//! boolean casts, because that can be e.g. -1 -> TRUE -> 1. This is necessary to prevent some optimizer bugs.
	static bool CastIsInvertible(const LogicalType &source_type, const LogicalType &target_type);

	string ToString() const override;

	bool Equals(const BaseExpression &other) const override;

	unique_ptr<Expression> Copy() override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<Expression> Deserialize(Deserializer &deserializer);

private:
	BoundCastExpression(ClientContext &context, unique_ptr<Expression> child, LogicalType target_type);
};
} // namespace duckdb
