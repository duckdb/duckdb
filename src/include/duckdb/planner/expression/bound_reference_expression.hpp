//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/bound_reference_expression.hpp
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

//! A BoundReferenceExpression represents a physical index into a DataChunk
class BoundReferenceExpression : public Expression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::BOUND_REF;

public:
	BoundReferenceExpression(string alias, LogicalType type, idx_t index);
	BoundReferenceExpression(LogicalType type, storage_t index);

	//! Index used to access data in the chunks
	storage_t index;

public:
	bool IsScalar() const override {
		return false;
	}
	bool IsFoldable() const override {
		return false;
	}

	string ToString() const override;

	hash_t Hash() const override;
	bool Equals(const BaseExpression &other) const override;

	unique_ptr<Expression> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<Expression> Deserialize(Deserializer &deserializer);
};
} // namespace duckdb
