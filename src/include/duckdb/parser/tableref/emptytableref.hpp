//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/tableref/emptytableref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "duckdb/parser/tableref.hpp"
#include "duckdb/common/enums/tableref_type.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {
class Deserializer;
class Serializer;

class EmptyTableRef : public TableRef {
public:
	static constexpr const TableReferenceType TYPE = TableReferenceType::EMPTY_FROM;

public:
	EmptyTableRef() : TableRef(TableReferenceType::EMPTY_FROM) {
	}

public:
	string ToString() const override;
	bool Equals(const TableRef &other_p) const override;

	unique_ptr<TableRef> Copy() override;

	//! Deserializes a blob back into a DummyTableRef
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableRef> Deserialize(Deserializer &source);
};
} // namespace duckdb
