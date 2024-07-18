//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/tableref/column_data_ref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/tableref.hpp"
#include "duckdb/common/optionally_owned_ptr.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"

namespace duckdb {

//! Represents a TableReference to a materialized result
class ColumnDataRef : public TableRef {
public:
	static constexpr const TableReferenceType TYPE = TableReferenceType::COLUMN_DATA;

public:
	explicit ColumnDataRef(shared_ptr<ColumnDataCollection> collection)
	    : TableRef(TableReferenceType::COLUMN_DATA), collection(std::move(collection)) {
	}
	ColumnDataRef(shared_ptr<ColumnDataCollection> collection, vector<string> expected_names)
	    : TableRef(TableReferenceType::COLUMN_DATA), expected_names(std::move(expected_names)),
	      collection(std::move(collection)) {
	}

public:
	//! The set of expected names
	vector<string> expected_names;
	//! The collection to scan
	shared_ptr<ColumnDataCollection> collection;

public:
	string ToString() const override;
	bool Equals(const TableRef &other_p) const override;

	unique_ptr<TableRef> Copy() override;

	//! Deserializes a blob back into a ColumnDataRef
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableRef> Deserialize(Deserializer &source);
};

} // namespace duckdb
