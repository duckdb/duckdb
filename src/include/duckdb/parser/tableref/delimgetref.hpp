//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/tableref/delimgetref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/tableref.hpp"

namespace duckdb {

class DelimGetRef : public TableRef {

public:
	DelimGetRef() : TableRef(TableReferenceType::DELIM_GET) {
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
