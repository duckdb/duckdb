//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/tableref/pos_join_ref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/tableref.hpp"

namespace duckdb {
//! Represents a cross product
class PositionalJoinRef : public TableRef {
public:
	PositionalJoinRef() : TableRef(TableReferenceType::POSITIONAL_JOIN) {
	}

	//! The left hand side of the cross product
	unique_ptr<TableRef> left;
	//! The right hand side of the cross product
	unique_ptr<TableRef> right;

public:
	string ToString() const override;
	bool Equals(const TableRef *other_p) const override;

	unique_ptr<TableRef> Copy() override;

	//! Serializes a blob into a PositionalJoinRef
	void Serialize(FieldWriter &serializer) const override;
	//! Deserializes a blob back into a PositionalJoinRef
	static unique_ptr<TableRef> Deserialize(FieldReader &source);
};
} // namespace duckdb
