//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/tableref/dummytableref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/tableref.hpp"

namespace duckdb {
//! Represents a cross product
class DummyTableRef : public TableRef {
public:
	DummyTableRef() : TableRef(TableReferenceType::DUMMY) {
	}

public:
	bool Equals(const TableRef *other_) const override;

	unique_ptr<TableRef> Copy() override;

	//! Serializes a blob into a DummyTableRef
	void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into a DummyTableRef
	static unique_ptr<TableRef> Deserialize(Deserializer &source);
};
} // namespace duckdb
