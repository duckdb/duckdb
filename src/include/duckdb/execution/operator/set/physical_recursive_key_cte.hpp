//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/set/physical_recursive_key_cte.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/set/physical_recursive_cte.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

namespace duckdb {

class RecursiveKeyCTEState;

class PhysicalRecursiveKeyCTE : public PhysicalRecursiveCTE {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::RECURSIVE_KEY_CTE;

public:
	PhysicalRecursiveKeyCTE(string ctename, idx_t table_index, vector<LogicalType> types, bool union_all,
	                        unique_ptr<PhysicalOperator> top, unique_ptr<PhysicalOperator> bottom,
	                        idx_t estimated_cardinality);
	~PhysicalRecursiveKeyCTE() override;
	// Contains the result of the key variant
	std::shared_ptr<ColumnDataCollection> recurring_table;
	// Contains the types of the payload and key columns.
	vector<LogicalType> payload_types, distinct_types;
	// Contains the payload and key indices
	vector<idx_t> payload_idx, distinct_idx;
	// Contains the aggregates for the payload
	vector<unique_ptr<BoundAggregateExpression>> payload_aggregates;

public:
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

protected:
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
};
} // namespace duckdb
