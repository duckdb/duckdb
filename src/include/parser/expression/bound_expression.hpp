//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/expression/bound_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression.hpp"
#include "parser/sql_node_visitor.hpp"

namespace duckdb {

//! A BoundExpression represents a physical index into
class BoundExpression : public Expression {
public:
	BoundExpression(TypeId type, uint32_t index, uint32_t depth = 0)
	    : Expression(ExpressionType::BOUND_REF, type), index(index), depth(depth) {
	}

	ExpressionClass GetExpressionClass() override {
		return ExpressionClass::BOUND_REF;
	}

	unique_ptr<Expression> Copy() override;

	//! Serializes an Expression to a stand-alone binary blob
	void Serialize(Serializer &serializer) override;

	uint64_t Hash() const override;
	bool Equals(const Expression *other) const override;

	//! Index used to access data in the chunks
	uint32_t index;
	//! Subquery recursion depth, needed for execution
	uint32_t depth;

	string ToString() const override;

	bool IsScalar() override {
		return false;
	}
};
} // namespace duckdb
