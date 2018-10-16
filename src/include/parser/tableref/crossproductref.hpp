//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/tableref/crossproductref.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/sql_node_visitor.hpp"
#include "parser/tableref.hpp"

namespace duckdb {
//! Represents a cross product
class CrossProductRef : public TableRef {
  public:
	CrossProductRef() : TableRef(TableReferenceType::CROSS_PRODUCT) {}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }

	virtual std::unique_ptr<TableRef> Copy() override;

	//! Serializes a blob into a CrossProductRef
	virtual void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into a CrossProductRef
	static std::unique_ptr<TableRef> Deserialize(Deserializer &source);

	//! The left hand side of the cross product
	std::unique_ptr<TableRef> left;
	//! The right hand side of the cross product
	std::unique_ptr<TableRef> right;
};
} // namespace duckdb
