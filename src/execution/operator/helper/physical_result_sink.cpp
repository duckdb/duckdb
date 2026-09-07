#include "duckdb/execution/operator/helper/physical_result_sink.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/thread_annotation.hpp"
#include "duckdb/common/types/batched_data_collection.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/main/buffered_data/batched_buffered_data.hpp"
#include "duckdb/main/buffered_data/simple_buffered_data.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/stream_query_result.hpp"

namespace duckdb {

PhysicalResultSink::PhysicalResultSink(PhysicalPlan &physical_plan, PreparedStatementData &data,
                                       ResultLifetime lifetime, ResultOrdering ordering)
    : PhysicalResultCollector(physical_plan, data), lifetime(lifetime), ordering(ordering) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class ResultSinkGlobalState : public GlobalSinkState {
public:
	//! This is weak to avoid creating a cyclical reference
	weak_ptr<ClientContext> context;
	//! The buffer behind a stream result. It also holds the retention decision, so it exists
	//! whenever the plan left retention open. Null for a sink retained by the plan
	shared_ptr<BufferedData> buffered_data;
	annotated_mutex glock;
	//! CDC to materialize a result in arrival order
	unique_ptr<ColumnDataCollection> collection DUCKDB_GUARDED_BY(glock);
	//! CDC to materialize a result in batch order
	unique_ptr<BatchedDataCollection> batch_data DUCKDB_GUARDED_BY(glock);
};

class ResultSinkLocalState : public LocalSinkState {
public:
	//! Set when a park deposited the chunk, so the re-delivery is not appended again. Parks deposit
	//! so that a parked producer always implies a poppable chunk
	bool chunk_deposited = false;
	//! The batch this producer is currently sinking
	idx_t current_batch = 0;
	//! Local CDC (arrival order) that will be merged later, in Combine
	unique_ptr<ColumnDataCollection> collection;
	ColumnDataAppendState append_state;
	//! Local CDC (batch order) that will be merged later, in Combine
	unique_ptr<BatchedDataCollection> batch_data;
};

unique_ptr<GlobalSinkState> PhysicalResultSink::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_uniq<ResultSinkGlobalState>();
	state->context = context.shared_from_this();
	if (lifetime != ResultLifetime::RETAINED) {
		if (BatchOrdered()) {
			state->buffered_data = make_shared_ptr<BatchedBufferedData>(context, lifetime);
		} else {
			state->buffered_data = make_shared_ptr<SimpleBufferedData>(context, lifetime);
		}
	}
	return std::move(state);
}

unique_ptr<LocalSinkState> PhysicalResultSink::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<ResultSinkLocalState>();
}

ResultLifetime PhysicalResultSink::CurrentLifetime(ResultSinkGlobalState &gstate) const {
	if (!gstate.buffered_data) {
		return ResultLifetime::RETAINED;
	}
	return gstate.buffered_data->Lifetime();
}

bool PhysicalResultSink::DrainsByBatchIndex(ResultSinkGlobalState &gstate) const {
	return BatchOrdered() && CurrentLifetime(gstate) != ResultLifetime::RETAINED;
}

SinkResultType PhysicalResultSink::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<ResultSinkGlobalState>();
	auto &lstate = input.local_state.Cast<ResultSinkLocalState>();
	auto current = CurrentLifetime(gstate);
	if (current == ResultLifetime::UNDECIDED) {
		// The first chunk waits unconsumed for the consumer's choice; the pipeline re-delivers it on resume
		if (gstate.buffered_data->ParkUndecided(input.interrupt_state)) {
			return SinkResultType::BLOCKED;
		}
		current = gstate.buffered_data->Lifetime();
	}
	if (current == ResultLifetime::RETAINED) {
		return SinkRetained(context, lstate, chunk);
	}
	return SinkDraining(gstate, lstate, chunk, input);
}

SinkResultType PhysicalResultSink::SinkRetained(ExecutionContext &context, ResultSinkLocalState &lstate,
                                                DataChunk &chunk) const {
	if (BatchOrdered()) {
		if (!lstate.batch_data) {
			lstate.batch_data = make_uniq<BatchedDataCollection>(context.client, types, memory_type);
		}
		lstate.batch_data->Append(chunk, lstate.partition_info.batch_index.GetIndex());
	} else {
		if (!lstate.collection) {
			lstate.collection = CreateCollection(context.client);
			lstate.collection->InitializeAppend(lstate.append_state);
		}
		lstate.collection->Append(lstate.append_state, chunk);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkResultType PhysicalResultSink::SinkDraining(ResultSinkGlobalState &gstate, ResultSinkLocalState &lstate,
                                                DataChunk &chunk, OperatorSinkInput &input) const {
	if (lstate.chunk_deposited) {
		lstate.chunk_deposited = false;
		return SinkResultType::NEED_MORE_INPUT;
	}
	if (BatchOrdered()) {
		auto batch = lstate.partition_info.batch_index.GetIndex();
		auto min_batch_index = lstate.partition_info.min_batch_index.GetIndex();
		lstate.current_batch = batch;
		auto &buffered_data = gstate.buffered_data->Cast<BatchedBufferedData>();
		buffered_data.UpdateMinBatchIndex(min_batch_index);
		if (buffered_data.AppendOrBlock(chunk, batch, input.interrupt_state)) {
			lstate.chunk_deposited = true;
			return SinkResultType::BLOCKED;
		}
		return SinkResultType::NEED_MORE_INPUT;
	}
	auto &buffered_data = gstate.buffered_data->Cast<SimpleBufferedData>();
	if (buffered_data.AppendOrBlock(chunk, input.interrupt_state)) {
		lstate.chunk_deposited = true;
		return SinkResultType::BLOCKED;
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalResultSink::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<ResultSinkGlobalState>();
	auto &lstate = input.local_state.Cast<ResultSinkLocalState>();
	if (CurrentLifetime(gstate) == ResultLifetime::RETAINED) {
		return CombineRetained(gstate, lstate);
	}
	return CombineDraining(gstate, lstate);
}

SinkCombineResultType PhysicalResultSink::CombineDraining(ResultSinkGlobalState &gstate,
                                                          ResultSinkLocalState &lstate) const {
	if (BatchOrdered()) {
		auto min_batch_index = lstate.partition_info.min_batch_index.GetIndex();
		gstate.buffered_data->Cast<BatchedBufferedData>().UpdateMinBatchIndex(min_batch_index);
	}
	return SinkCombineResultType::FINISHED;
}

SinkCombineResultType PhysicalResultSink::CombineRetained(ResultSinkGlobalState &gstate,
                                                          ResultSinkLocalState &lstate) const {
	// A producer whose partition held no rows never created its local collection
	if (BatchOrdered()) {
		if (!lstate.batch_data) {
			return SinkCombineResultType::FINISHED;
		}
		annotated_lock_guard<annotated_mutex> l(gstate.glock);
		if (!gstate.batch_data) {
			gstate.batch_data = std::move(lstate.batch_data);
		} else {
			gstate.batch_data->Merge(*lstate.batch_data);
		}
		return SinkCombineResultType::FINISHED;
	}
	if (!lstate.collection || lstate.collection->Count() == 0) {
		return SinkCombineResultType::FINISHED;
	}
	annotated_lock_guard<annotated_mutex> l(gstate.glock);
	if (!gstate.collection) {
		gstate.collection = std::move(lstate.collection);
	} else {
		gstate.collection->Combine(*lstate.collection);
	}
	return SinkCombineResultType::FINISHED;
}

SinkNextBatchType PhysicalResultSink::NextBatch(ExecutionContext &context, OperatorSinkNextBatchInput &input) const {
	auto &gstate = input.global_state.Cast<ResultSinkGlobalState>();
	if (!DrainsByBatchIndex(gstate)) {
		return SinkNextBatchType::READY;
	}
	auto &lstate = input.local_state.Cast<ResultSinkLocalState>();

	auto batch = lstate.current_batch;
	auto min_batch_index = lstate.partition_info.min_batch_index.GetIndex();
	auto new_index = lstate.partition_info.batch_index.GetIndex();

	auto &buffered_data = gstate.buffered_data->Cast<BatchedBufferedData>();
	buffered_data.CompleteBatch(batch);
	lstate.current_batch = new_index;
	buffered_data.UpdateMinBatchIndex(min_batch_index);
	return SinkNextBatchType::READY;
}

SinkNextBatchType PhysicalResultSink::UpdateMinBatchIndex(ExecutionContext &context,
                                                          OperatorSinkNextBatchInput &input) const {
	auto &gstate = input.global_state.Cast<ResultSinkGlobalState>();
	if (!DrainsByBatchIndex(gstate)) {
		return SinkNextBatchType::READY;
	}
	auto min_batch_index = input.local_state.partition_info.min_batch_index.GetIndex();
	gstate.buffered_data->Cast<BatchedBufferedData>().UpdateMinBatchIndex(min_batch_index);
	return SinkNextBatchType::READY;
}

unique_ptr<QueryResult> PhysicalResultSink::GetResult(GlobalSinkState &state) const {
	auto &gstate = state.Cast<ResultSinkGlobalState>();
	if (CurrentLifetime(gstate) == ResultLifetime::RETAINED) {
		return GetMaterializedResult(gstate);
	}
	return GetStreamResult(gstate);
}

unique_ptr<QueryResult> PhysicalResultSink::GetStreamResult(ResultSinkGlobalState &gstate) const {
	auto cc = gstate.context.lock();
	if (!cc) {
		throw ConnectionException("Connection has already been closed");
	}
	return make_uniq<StreamQueryResult>(statement_type, properties, types, names, cc->GetClientProperties(),
	                                    gstate.buffered_data);
}

unique_ptr<QueryResult> PhysicalResultSink::GetMaterializedResult(ResultSinkGlobalState &gstate) const {
	auto cc = gstate.context.lock();
	if (!cc) {
		throw ConnectionException("Connection has already been closed");
	}
	unique_ptr<ColumnDataCollection> collection;
	{
		annotated_lock_guard<annotated_mutex> l(gstate.glock);
		if (BatchOrdered()) {
			if (gstate.batch_data) {
				collection = gstate.batch_data->FetchCollection();
			}
		} else {
			collection = std::move(gstate.collection);
		}
	}
	if (!collection) {
		collection = CreateCollection(*cc);
	}
	return make_uniq<MaterializedQueryResult>(statement_type, properties, names, std::move(collection),
	                                          cc->GetClientProperties());
}

bool PhysicalResultSink::HasBlockedResultProducer(GlobalSinkState &state) const {
	auto &gstate = state.Cast<ResultSinkGlobalState>();
	if (!gstate.buffered_data) {
		return false;
	}
	return gstate.buffered_data->HasParkedProducer();
}

OperatorPartitionInfo PhysicalResultSink::RequiredPartitionInfo() const {
	if (BatchOrdered()) {
		return OperatorPartitionInfo::BatchIndex();
	}
	return PhysicalOperator::RequiredPartitionInfo();
}

bool PhysicalResultSink::ParallelSink() const {
	// Source order is preserved by keeping the sink single-threaded
	return ordering != ResultOrdering::SOURCE_ORDERED;
}

bool PhysicalResultSink::SinkOrderDependent() const {
	return !BatchOrdered();
}

bool PhysicalResultSink::IsStreaming() const {
	return lifetime != ResultLifetime::RETAINED;
}

PipelineExternalInputSupport PhysicalResultSink::GetExternalInputSupport() const {
	if (BatchOrdered()) {
		return PipelineExternalInputSupport::SUPPORTED;
	}
	return PhysicalOperator::GetExternalInputSupport();
}

} // namespace duckdb
