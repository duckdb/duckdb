#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/arrow/physical_arrow_collector.hpp"
#include "duckdb/common/arrow/physical_arrow_batch_collector.hpp"
#include "duckdb/common/arrow/arrow_query_result.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

unique_ptr<PhysicalResultCollector> PhysicalArrowCollector::Create(ClientContext &context, PreparedStatementData &data,
                                                                   idx_t batch_size) {
	if (!PhysicalPlanGenerator::PreserveInsertionOrder(context, *data.plan)) {
		// the plan is not order preserving, so we just use the parallel materialized collector
		return make_uniq_base<PhysicalResultCollector, PhysicalArrowCollector>(data, true, batch_size);
	} else if (!PhysicalPlanGenerator::UseBatchIndex(context, *data.plan)) {
		// the plan is order preserving, but we cannot use the batch index: use a single-threaded result collector
		return make_uniq_base<PhysicalResultCollector, PhysicalArrowCollector>(data, false, batch_size);
	} else {
		return make_uniq_base<PhysicalResultCollector, PhysicalArrowBatchCollector>(data, batch_size);
	}
}

SinkResultType PhysicalArrowCollector::Sink(ExecutionContext &context, DataChunk &chunk,
                                            OperatorSinkInput &input) const {
	auto &lstate = input.local_state.Cast<ArrowCollectorLocalState>();
	// Append to the appender, up to chunk size

	auto count = chunk.size();
	auto &appender = lstate.appender;
	D_ASSERT(count != 0);

	idx_t processed = 0;
	do {
		if (!appender) {
			// Create the appender if we haven't started this chunk yet
			auto properties = context.client.GetClientProperties();
			D_ASSERT(processed < count);
			auto initial_capacity = MinValue(record_batch_size, count - processed);
			appender = make_uniq<ArrowAppender>(types, initial_capacity, properties);
		}

		// Figure out how much we can still append to this chunk
		auto row_count = appender->RowCount();
		D_ASSERT(record_batch_size > row_count);
		auto to_append = MinValue(record_batch_size - row_count, count - processed);

		// Append and check if the chunk is finished
		appender->Append(chunk, processed, processed + to_append, count);
		processed += to_append;
		row_count = appender->RowCount();
		if (row_count >= record_batch_size) {
			lstate.FinishArray();
		}
	} while (processed < count);
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalArrowCollector::Combine(ExecutionContext &context,
                                                      OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<ArrowCollectorGlobalState>();
	auto &lstate = input.local_state.Cast<ArrowCollectorLocalState>();
	auto &last_appender = lstate.appender;
	auto &arrays = lstate.finished_arrays;
	if (arrays.empty() && !last_appender) {
		// Nothing to do
		return SinkCombineResultType::FINISHED;
	}
	if (last_appender) {
		// FIXME: we could set these aside and merge them in a finalize event in an effort to create more balanced
		// chunks out of these remnants
		lstate.FinishArray();
	}
	// Collect all the finished arrays
	lock_guard<mutex> l(gstate.glock);
	// Move the arrays from our local state into the global state
	gstate.chunks.insert(gstate.chunks.end(), std::make_move_iterator(arrays.begin()),
	                     std::make_move_iterator(arrays.end()));
	arrays.clear();
	gstate.tuple_count += lstate.tuple_count;
	return SinkCombineResultType::FINISHED;
}

unique_ptr<QueryResult> PhysicalArrowCollector::GetResult(GlobalSinkState &state_p) {
	auto &gstate = state_p.Cast<ArrowCollectorGlobalState>();
	return std::move(gstate.result);
}

unique_ptr<GlobalSinkState> PhysicalArrowCollector::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<ArrowCollectorGlobalState>();
}

unique_ptr<LocalSinkState> PhysicalArrowCollector::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<ArrowCollectorLocalState>();
}

SinkFinalizeType PhysicalArrowCollector::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                  OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<ArrowCollectorGlobalState>();

	if (gstate.chunks.empty()) {
		if (gstate.tuple_count != 0) {
			throw InternalException(
			    "PhysicalArrowCollector Finalize contains no chunks, but tuple_count is non-zero (%d)",
			    gstate.tuple_count);
		}
		gstate.result = make_uniq<ArrowQueryResult>(statement_type, properties, names, types,
		                                            context.GetClientProperties(), record_batch_size);
		return SinkFinalizeType::READY;
	}

	gstate.result = make_uniq<ArrowQueryResult>(statement_type, properties, names, types, context.GetClientProperties(),
	                                            record_batch_size);
	auto &arrow_result = gstate.result->Cast<ArrowQueryResult>();
	arrow_result.SetArrowData(std::move(gstate.chunks));

	return SinkFinalizeType::READY;
}

bool PhysicalArrowCollector::ParallelSink() const {
	return parallel;
}

bool PhysicalArrowCollector::SinkOrderDependent() const {
	return true;
}

} // namespace duckdb
