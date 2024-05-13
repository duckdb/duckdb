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
	explicit ColumnDataRef(ColumnDataCollection &collection)
	    : TableRef(TableReferenceType::COLUMN_DATA), collection(collection) {
	}
	ColumnDataRef(vector<string> expected_names, optionally_owned_ptr<ColumnDataCollection> collection_p)
	    : TableRef(TableReferenceType::COLUMN_DATA), collection(std::move(collection_p)),
	      expected_names(std::move(expected_names)) {
	}

public:
	//! (optionally owned) materialized column data
	optionally_owned_ptr<ColumnDataCollection> collection;
	//! The set of expected names
	vector<string> expected_names;

public:
	string ToString() const override;
	bool Equals(const TableRef &other_p) const override;

	unique_ptr<TableRef> Copy() override;

	//! Deserializes a blob back into a ColumnDataRef
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableRef> Deserialize(Deserializer &source);
};
} // namespace duckdb
