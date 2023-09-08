//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_update.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {
class TableCatalogEntry;

class LogicalUpdate : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_UPDATE;

public:
	explicit LogicalUpdate(TableCatalogEntry &table);

	//! The base table to update
	TableCatalogEntry &table;
	//! table catalog index
	idx_t table_index;
	//! if returning option is used, return the update chunk
	bool return_chunk;
	vector<PhysicalIndex> columns;
	vector<unique_ptr<Expression>> bound_defaults;
	bool update_is_del_and_insert;

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

	idx_t EstimateCardinality(ClientContext &context) override;
	string GetName() const override;

protected:
	vector<ColumnBinding> GetColumnBindings() override;
	void ResolveTypes() override;

private:
	LogicalUpdate(ClientContext &context, const string &catalog, const string &schema, const string &table);
};
} // namespace duckdb
