//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/tableref/crossproductref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/tableref.hpp"

namespace duckdb {
//! Represents a cross product
class CrossProductRef : public TableRef {
public:
	CrossProductRef() : TableRef(TableReferenceType::CROSS_PRODUCT) {
	}

	//! The left hand side of the cross product
	unique_ptr<TableRef> left;
	//! The right hand side of the cross product
	unique_ptr<TableRef> right;

public:
	bool Equals(const TableRef *other_) const override;

	unique_ptr<TableRef> Copy() override;

	//! Serializes a blob into a CrossProductRef
	void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into a CrossProductRef
	static unique_ptr<TableRef> Deserialize(Deserializer &source);
};
} // namespace duckdb
