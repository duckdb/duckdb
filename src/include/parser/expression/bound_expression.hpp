//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/expression/bound_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression.hpp"

namespace duckdb {

//! A BoundExpression represents a physical index into a DataChunk
class BoundExpression : public Expression {
public:
	BoundExpression(string alias, TypeId type, uint32_t index, uint32_t depth = 0)
	    : Expression(ExpressionType::BOUND_REF, type), index(index), depth(depth) {
		this->alias = alias;
	}
	BoundExpression(TypeId type, uint32_t index, uint32_t depth = 0) : BoundExpression(string(), type, index, depth) {
	}

	ExpressionClass GetExpressionClass() override {
		return ExpressionClass::BOUND_REF;
	}

	unique_ptr<Expression> Copy() override;

	//! Serializes an BoundExpression to a stand-alone binary blob
	void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into an BoundExpression
	static unique_ptr<Expression> Deserialize(ExpressionType type, TypeId return_type, Deserializer &source);

	uint64_t Hash() const override;
	bool Equals(const Expression *other) const override;

	string ToString() const override;

	bool IsScalar() override {
		return false;
	}

	//! Index used to access data in the chunks
	uint32_t index;
	//! The subquery depth, used for execution
	uint32_t depth;
};
} // namespace duckdb
