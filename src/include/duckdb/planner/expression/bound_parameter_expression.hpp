//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/bound_parameter_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/bound_parameter_map.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/shared_ptr_ipp.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/planner/expression/bound_parameter_data.hpp"

namespace duckdb {
class BaseExpression;
class Deserializer;
class Serializer;

class BoundParameterExpression : public Expression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::BOUND_PARAMETER;

public:
	explicit BoundParameterExpression(const string &identifier);

	string identifier;
	shared_ptr<BoundParameterData> parameter_data;

public:
	//! Invalidate a bound parameter expression - forcing a rebind on any subsequent filters
	DUCKDB_API static void Invalidate(Expression &expr);
	//! Invalidate all parameters within an expression
	DUCKDB_API static void InvalidateRecursive(Expression &expr);

	bool IsScalar() const override;
	bool HasParameter() const override;
	bool IsFoldable() const override;

	string ToString() const override;

	bool Equals(const BaseExpression &other) const override;
	hash_t Hash() const override;

	unique_ptr<Expression> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<Expression> Deserialize(Deserializer &deserializer);

private:
	BoundParameterExpression(bound_parameter_map_t &global_parameter_set, string identifier, LogicalType return_type,
	                         shared_ptr<BoundParameterData> parameter_data);
};

} // namespace duckdb
