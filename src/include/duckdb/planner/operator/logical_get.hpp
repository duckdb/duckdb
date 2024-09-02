//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_get.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/common/extra_operator_info.hpp"

namespace duckdb {
class DynamicTableFilterSet;

//! LogicalGet represents a scan operation from a data source
class LogicalGet : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_GET;

public:
	LogicalGet(idx_t table_index, TableFunction function, unique_ptr<FunctionData> bind_data,
	           vector<LogicalType> returned_types, vector<string> returned_names);

	//! The table index in the current bind context
	idx_t table_index;
	//! The function that is called
	TableFunction function;
	//! The bind data of the function
	unique_ptr<FunctionData> bind_data;
	//! The types of ALL columns that can be returned by the table function
	vector<LogicalType> returned_types;
	//! The names of ALL columns that can be returned by the table function
	vector<string> names;
	//! Columns that are used outside the scan
	vector<idx_t> projection_ids;
	//! Filters pushed down for table scan
	TableFilterSet table_filters;
	//! The set of input parameters for the table function
	vector<Value> parameters;
	//! The set of named input parameters for the table function
	named_parameter_map_t named_parameters;
	//! The set of named input table types for the table-in table-out function
	vector<LogicalType> input_table_types;
	//! The set of named input table names for the table-in table-out function
	vector<string> input_table_names;
	//! For a table-in-out function, the set of projected input columns
	vector<column_t> projected_input;
	//! Currently stores File Filters (as strings) applied by hive partitioning/complex filter pushdown
	//! Stored so they can be included in explain output
	ExtraOperatorInfo extra_info;
	//! Contains a reference to dynamically generated table filters (through e.g. a join up in the tree)
	shared_ptr<DynamicTableFilterSet> dynamic_filters;

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;
	//! Returns the underlying table that is being scanned, or nullptr if there is none
	optional_ptr<TableCatalogEntry> GetTable() const;

public:
	void SetColumnIds(vector<column_t> &&column_ids);
	void AddColumnId(column_t column_id);
	void ClearColumnIds();
	const vector<column_t> &GetColumnIds() const;
	vector<column_t> &GetMutableColumnIds();
	vector<ColumnBinding> GetColumnBindings() override;
	idx_t EstimateCardinality(ClientContext &context) override;

	vector<idx_t> GetTableIndex() const override;
	//! Skips the serialization check in VerifyPlan
	bool SupportSerialization() const override {
		return function.verify_serialization;
	}

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

protected:
	void ResolveTypes() override;

private:
	LogicalGet();

private:
	//! Bound column IDs
	vector<column_t> column_ids;
};
} // namespace duckdb
