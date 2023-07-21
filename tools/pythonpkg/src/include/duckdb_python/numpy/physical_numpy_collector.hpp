#pragma once

#include "duckdb/execution/operator/helper/physical_materialized_collector.hpp"
#include "duckdb_python/numpy/array_wrapper.hpp"
#include "duckdb/common/types/batched_data_collection.hpp"
#include "duckdb/common/map.hpp"

namespace duckdb {

class NumpyCollectorGlobalState : public MaterializedCollectorGlobalState {
public:
	~NumpyCollectorGlobalState() override {
		py::gil_scoped_acquire gil;
		result.reset();
		batches.clear();
	}

public:
	//! The result returned by GetResult
	unique_ptr<QueryResult> result;
	//! The unordered batches
	map<idx_t, unique_ptr<ColumnDataCollection>> batches;
	//! The number assigned to a batch in Combine
	idx_t batch_index = 0;
	//! The collection we will create from the batches
	unique_ptr<BatchedDataCollection> collection;
};

class PhysicalNumpyCollector : public PhysicalMaterializedCollector {
public:
	PhysicalNumpyCollector(PreparedStatementData &data, bool parallel) : PhysicalMaterializedCollector(data, parallel) {
	}

public:
	static unique_ptr<PhysicalResultCollector> Create(ClientContext &context, PreparedStatementData &data);
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	unique_ptr<QueryResult> GetResult(GlobalSinkState &state) override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
};

} // namespace duckdb
