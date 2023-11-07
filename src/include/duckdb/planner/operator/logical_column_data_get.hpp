//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_column_data_get.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalColumnDataGet represents a scan operation from a ColumnDataCollection
class LogicalColumnDataGet : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_CHUNK_GET;

public:
	LogicalColumnDataGet(idx_t table_index, vector<LogicalType> types, unique_ptr<ColumnDataCollection> collection);

	//! The table index in the current bind context
	idx_t table_index;
	//! The types of the chunk
	vector<LogicalType> chunk_types;
	//! The chunk collection to scan
	unique_ptr<ColumnDataCollection> collection;

public:
	vector<ColumnBinding> GetColumnBindings() override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

	vector<idx_t> GetTableIndex() const override;
	string GetName() const override;

protected:
	void ResolveTypes() override {
		// types are resolved in the constructor
		this->types = chunk_types;
	}
};
} // namespace duckdb
