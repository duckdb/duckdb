//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/set/physical_recursive_key_cte.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/set/physical_recursive_cte.hpp"

namespace duckdb {

class RecursiveKeyCTEState;

class PhysicalRecursiveKeyCTE : public PhysicalRecursiveCTE {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::RECURSIVE_KEY_CTE;

public:
	PhysicalRecursiveKeyCTE(string ctename, idx_t table_index, vector<LogicalType> types, bool union_all,
	                        vector<idx_t> key_columns, unique_ptr<PhysicalOperator> top,
	                        unique_ptr<PhysicalOperator> bottom, idx_t estimated_cardinality);
	~PhysicalRecursiveKeyCTE() override;

	vector<idx_t> key_columns;
	std::shared_ptr<ColumnDataCollection> recurring_table;

public:
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

protected:
	//! Probe Hash Table and eliminate duplicate rows
	idx_t ProbeHT(DataChunk &chunk, RecursiveKeyCTEState &state) const;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
};
} // namespace duckdb
