//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/aggregate/aggregate_object.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/aggregate_function.hpp"

namespace duckdb {

class BoundAggregateExpression;

struct AggregateObject {
	AggregateObject(AggregateFunction function, FunctionData *bind_data, idx_t child_count, idx_t payload_size,
	                bool distinct, PhysicalType return_type, Expression *filter = nullptr);
	AggregateObject(BoundAggregateExpression *aggr);

	AggregateFunction function;
	FunctionData *bind_data;
	idx_t child_count;
	idx_t payload_size;
	bool distinct;
	PhysicalType return_type;
	Expression *filter = nullptr;

	static vector<AggregateObject> CreateAggregateObjects(const vector<BoundAggregateExpression *> &bindings);
};

struct AggregateFilterData {
	AggregateFilterData(Allocator &allocator, Expression &filter_expr, const vector<LogicalType> &payload_types);

	idx_t ApplyFilter(DataChunk &payload);

	ExpressionExecutor filter_executor;
	DataChunk filtered_payload;
	SelectionVector true_sel;
};

struct AggregateFilterDataSet {
	AggregateFilterDataSet();

	vector<unique_ptr<AggregateFilterData>> filter_data;

public:
	void Initialize(Allocator &allocator, const vector<AggregateObject> &aggregates,
	                const vector<LogicalType> &payload_types);

	AggregateFilterData &GetFilterData(idx_t aggr_idx);
};

} // namespace duckdb
