#pragma once

#include "duckdb/execution/operator/helper/physical_result_collector.hpp"
#include "duckdb/execution/physical_operator_states.hpp"
#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/types/batched_data_collection.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"

namespace duckdb {

struct ArrowCollectorGlobalState : public GlobalSinkState {
public:
	//! The result returned by GetResult
	unique_ptr<QueryResult> result;
	vector<unique_ptr<ArrowArrayWrapper>> chunks;
	mutex glock;
	shared_ptr<ClientContext> context;
	idx_t tuple_count = 0;
};

struct ArrowCollectorLocalState : public LocalSinkState {
public:
	// The appender for the current chunk we're creating
	unique_ptr<ArrowAppender> appender;
	vector<unique_ptr<ArrowArrayWrapper>> finished_arrays;
	idx_t tuple_count = 0;

public:
	void FinishArray() {
		auto finished_array = make_uniq<ArrowArrayWrapper>();
		auto row_count = appender->RowCount();
		finished_array->arrow_array = appender->Finalize();
		appender.reset();
		finished_arrays.push_back(std::move(finished_array));
		tuple_count += row_count;
	}
};

class PhysicalArrowCollector : public PhysicalResultCollector {
public:
	PhysicalArrowCollector(PreparedStatementData &data, bool parallel, idx_t batch_size)
	    : PhysicalResultCollector(data), record_batch_size(batch_size), parallel(parallel) {
	}

public:
	static unique_ptr<PhysicalResultCollector> Create(ClientContext &context, PreparedStatementData &data,
	                                                  idx_t batch_size);
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	unique_ptr<QueryResult> GetResult(GlobalSinkState &state) override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	bool ParallelSink() const override;
	bool SinkOrderDependent() const override;

public:
	//! User provided batch size
	idx_t record_batch_size;
	bool parallel;
};

} // namespace duckdb
