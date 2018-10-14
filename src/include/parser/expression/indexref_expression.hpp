//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/expression/indexref_expression.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression.hpp"

namespace duckdb {

//! Represents a physical
class IndexRefExpression : public Expression {
  public:
	IndexRefExpression(int index, TypeId type = TypeId::INVALID)
	    : Expression(ExpressionType::INDEX_REF, type), index(index), depth(0) {}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }
	virtual ExpressionClass GetExpressionClass() override {
		return ExpressionClass::INDEX_REF;
	}

	//! Serializes an Expression to a stand-alone binary blob
	virtual void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into an ConstantExpression
	static std::unique_ptr<Expression>
	Deserialize(ExpressionDeserializeInformation *info, Deserializer &source);

	virtual void ResolveType() override;

	virtual bool Equals(const Expression *other_) override;

	//! Index used to access data in the chunks
	int index;
	//! Subquery recursion depth, needed for execution
	int depth = 0;

	virtual std::string ToString() const override;

	virtual bool IsScalar() override { return false; }
};
} // namespace duckdb
