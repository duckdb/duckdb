//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/tableref/emptytableref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/tableref.hpp"

namespace duckdb {
//! Represents a cross product
class EmptyTableRef : public TableRef {
public:
	EmptyTableRef() : TableRef(TableReferenceType::EMPTY) {
	}

public:
	string ToString() const override;
	bool Equals(const TableRef *other_p) const override;

	unique_ptr<TableRef> Copy() override;

	//! Serializes a blob into a DummyTableRef
	void Serialize(FieldWriter &serializer) const override;
	//! Deserializes a blob back into a DummyTableRef
	static unique_ptr<TableRef> Deserialize(FieldReader &source);
};
} // namespace duckdb
