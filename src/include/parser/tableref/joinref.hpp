//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/tableref/joinref.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression.hpp"
#include "parser/sql_node_visitor.hpp"
#include "parser/tableref.hpp"

namespace duckdb {
//! Represents a JOIN between two expressions
class JoinRef : public TableRef {
  public:
	JoinRef() : TableRef(TableReferenceType::JOIN) {}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }
	virtual bool Equals(const TableRef *other_) override {
		if (!TableRef::Equals(other_)) {
			return false;
		}
		auto other = (JoinRef *)other_;
		return left->Equals(other->left.get()) &&
		       right->Equals(other->right.get()) &&
		       condition->Equals(other->condition.get()) && type == other->type;
	}

	virtual std::unique_ptr<TableRef> Copy() override;

	//! Serializes a blob into a JoinRef
	virtual void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into a JoinRef
	static std::unique_ptr<TableRef> Deserialize(Deserializer &source);

	//! The left hand side of the join
	std::unique_ptr<TableRef> left;
	//! The right hand side of the join
	std::unique_ptr<TableRef> right;
	//! The join condition
	std::unique_ptr<Expression> condition;
	//! The join type
	JoinType type;
};
} // namespace duckdb
