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

class ManagedQueryResult;

//! Represents a TableReference to a materialized result
class ColumnDataRef : public TableRef {
public:
	static constexpr const TableReferenceType TYPE = TableReferenceType::COLUMN_DATA;

public:
	explicit ColumnDataRef(optionally_owned_ptr<ColumnDataCollection> collection_p,
	                       vector<string> expected_names = vector<string>());
	explicit ColumnDataRef(shared_ptr<ManagedQueryResult> managed_result_p,
	                       vector<string> expected_names = vector<string>());

public:
	//! The set of expected names
	vector<string> expected_names;

private:
	//! (Optionally) the owned collection
	optionally_owned_ptr<ColumnDataCollection> collection;
	//! (Optional) the managed query result this reads
	shared_ptr<ManagedQueryResult> managed_result;

public:
	optionally_owned_ptr<ColumnDataCollection> &Collection();
	const optionally_owned_ptr<ColumnDataCollection> &Collection() const;

	string ToString() const override;
	bool Equals(const TableRef &other_p) const override;

	unique_ptr<TableRef> Copy() override;

	//! Deserializes a blob back into a ColumnDataRef
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableRef> Deserialize(Deserializer &source);
};

} // namespace duckdb
