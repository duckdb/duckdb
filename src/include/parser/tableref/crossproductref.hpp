//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/tableref/crossproductref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/sql_node_visitor.hpp"
#include "parser/tableref.hpp"

namespace duckdb {
//! Represents a cross product
class CrossProductRef : public TableRef {
public:
	CrossProductRef() : TableRef(TableReferenceType::CROSS_PRODUCT) {
	}

	unique_ptr<TableRef> Accept(SQLNodeVisitor *v) override {
		return v->Visit(*this);
	}
	bool Equals(const TableRef *other_) override {
		if (!TableRef::Equals(other_)) {
			return false;
		}
		auto other = (CrossProductRef *)other_;
		return left->Equals(other->left.get()) && right->Equals(other->right.get());
	}

	unique_ptr<TableRef> Copy() override;

	//! Serializes a blob into a CrossProductRef
	void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into a CrossProductRef
	static unique_ptr<TableRef> Deserialize(Deserializer &source);

	//! The left hand side of the cross product
	unique_ptr<TableRef> left;
	//! The right hand side of the cross product
	unique_ptr<TableRef> right;
};
} // namespace duckdb
