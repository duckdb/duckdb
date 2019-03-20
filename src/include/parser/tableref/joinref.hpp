//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/tableref/joinref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression.hpp"
#include "parser/sql_node_visitor.hpp"
#include "parser/tableref.hpp"

#include <unordered_set>

namespace duckdb {
//! Represents a JOIN between two expressions
class JoinRef : public TableRef {
public:
	JoinRef() : TableRef(TableReferenceType::JOIN) {
	}

	unique_ptr<TableRef> Accept(SQLNodeVisitor *v) override {
		return v->Visit(*this);
	}
	bool Equals(const TableRef *other_) const override {
		if (!TableRef::Equals(other_)) {
			return false;
		}
		auto other = (JoinRef *)other_;
		return left->Equals(other->left.get()) && right->Equals(other->right.get()) &&
		       condition->Equals(other->condition.get()) && type == other->type;
	}

	unique_ptr<TableRef> Copy() override;

	//! Serializes a blob into a JoinRef
	void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into a JoinRef
	static unique_ptr<TableRef> Deserialize(Deserializer &source);

	//! The left hand side of the join
	unique_ptr<TableRef> left;
	//! The right hand side of the join
	unique_ptr<TableRef> right;
	//! The join condition
	unique_ptr<Expression> condition;
	//! The join type
	JoinType type;

	std::unordered_set<string> using_hidden_columns;
};
} // namespace duckdb
