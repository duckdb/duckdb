//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/tableref/column_data_ref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "duckdb/parser/tableref.hpp"
#include "duckdb/common/optionally_owned_ptr.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/enums/tableref_type.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class Deserializer;
class Serializer;

//! Represents a TableReference to a materialized result
class ColumnDataRef : public TableRef {
public:
	static constexpr const TableReferenceType TYPE = TableReferenceType::COLUMN_DATA;

public:
	explicit ColumnDataRef(optionally_owned_ptr<ColumnDataCollection> collection_p,
	                       vector<string> expected_names = vector<string>());

public:
	//! The set of expected names
	vector<string> expected_names;
	//! (Optionally) the owned collection
	optionally_owned_ptr<ColumnDataCollection> collection;

public:
	string ToString() const override;
	bool Equals(const TableRef &other_p) const override;

	unique_ptr<TableRef> Copy() override;

	//! Deserializes a blob back into a ColumnDataRef
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableRef> Deserialize(Deserializer &source);
};

} // namespace duckdb
