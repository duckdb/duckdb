//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/bound_operator_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "duckdb/planner/expression.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class BaseExpression;
class Deserializer;
class Serializer;

class BoundOperatorExpression : public Expression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::BOUND_OPERATOR;

public:
	BoundOperatorExpression(ExpressionType type, LogicalType return_type);

	vector<unique_ptr<Expression>> children;

public:
	string ToString() const override;

	bool Equals(const BaseExpression &other) const override;

	unique_ptr<Expression> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<Expression> Deserialize(Deserializer &deserializer);
};
} // namespace duckdb
