#include "duckdb/execution/operator/helper/physical_result_sink.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/thread_annotation.hpp"
#include "duckdb/common/types/batched_data_collection.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/main/buffered_data/batched_buffered_data.hpp"
#include "duckdb/main/buffered_data/simple_buffered_data.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/result_format.hpp"
#include "duckdb/main/result_unit.hpp"

namespace duckdb {

PhysicalResultSink::PhysicalResultSink(PhysicalPlan &physical_plan, PreparedStatementData &data,
                                       ResultLifetime lifetime, ResultOrdering ordering)
    : PhysicalResultCollector(physical_plan, data), lifetime(lifetime), ordering(ordering) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
//! A unit does not know the batch it came from, so a retained result keeps it alongside
struct RetainedUnit {
	idx_t batch;
	unique_ptr<ResultUnit> unit;
};

class ResultSinkGlobalState : public GlobalSinkState {
public:
	//! This is weak to avoid creating a cyclical reference
	weak_ptr<ClientContext> context;
	//! The buffer behind a stream result. It also holds the retention decision and the settled format,
	//! so it exists whenever the plan left retention open. Null for a sink retained by the plan
	shared_ptr<BufferedData> buffered_data;
	annotated_mutex glock;
	//! CDC to materialize a result in arrival order
	unique_ptr<ColumnDataCollection> collection DUCKDB_GUARDED_BY(glock);
	//! CDC to materialize a result in batch order
	unique_ptr<BatchedDataCollection> batch_data DUCKDB_GUARDED_BY(glock);
	//! The finished units of a retained result in any other format, merged from the producers
	vector<RetainedUnit> units DUCKDB_GUARDED_BY(glock);
};

class ResultSinkLocalState : public LocalSinkState {
public:
	//! Set when a park deposited the chunk, so the re-delivery is not appended again. Parks deposit
	//! so that a parked producer always implies a poppable unit
	bool chunk_deposited = false;
	//! The batch this producer is currently sinking
	idx_t current_batch = 0;
	//! Local CDC (arrival order) that will be merged later, in Combine
	unique_ptr<ColumnDataCollection> collection;
	ColumnDataAppendState append_state;
	//! Local CDC (batch order) that will be merged later, in Combine
	unique_ptr<BatchedDataCollection> batch_data;
	//! The format's per-producer state. Created at the first Append, because the retention (and with
	//! it the format) is not settled when the local sink state is
	unique_ptr<ResultFormatLocalState> format_state;
	//! Finished units of a retained result in a non-chunk format, merged into the global state in Combine
	vector<RetainedUnit> units;
};

void PhysicalResultSink::SetResultBuffer(shared_ptr<BufferedData> buffer) {
	D_ASSERT(!result_buffer);
	result_buffer = std::move(buffer);
}

unique_ptr<GlobalSinkState> PhysicalResultSink::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_uniq<ResultSinkGlobalState>();
	state->context = context.shared_from_this();
	if (lifetime != ResultLifetime::RETAINED) {
		D_ASSERT(result_buffer);
		state->buffered_data = result_buffer;
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

bool PhysicalResultSink::UsesChunkFormat(ResultSinkGlobalState &gstate) const {
	if (!gstate.buffered_data) {
		return true;
	}
	return gstate.buffered_data->Format().IsChunk();
}

const ChunkFormat &PhysicalResultSink::ChunkFormatOf(ResultSinkGlobalState &gstate) const {
	if (!gstate.buffered_data) {
		return ResultFormat::Chunk()->Cast<ChunkFormat>();
	}
	return gstate.buffered_data->Format().Cast<ChunkFormat>();
}

ResultFormatLocalState &PhysicalResultSink::LocalFormatState(ResultSinkGlobalState &gstate,
                                                             ResultSinkLocalState &lstate) const {
	if (!lstate.format_state) {
		auto &format = gstate.buffered_data->Format();
		lstate.format_state = format.InitLocal(gstate.buffered_data->FormatState());
	}
	return *lstate.format_state;
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
		if (UsesChunkFormat(gstate)) {
			return SinkRetained(context, gstate, lstate, chunk);
		}
		return SinkRetainedFormatted(gstate, lstate, chunk);
	}
	return SinkDraining(gstate, lstate, chunk, input);
}

SinkResultType PhysicalResultSink::SinkRetained(ExecutionContext &context, ResultSinkGlobalState &gstate,
                                                ResultSinkLocalState &lstate, DataChunk &chunk) const {
	if (BatchOrdered()) {
		if (!lstate.batch_data) {
			lstate.batch_data = ChunkFormatOf(gstate).CreateBatchedCollection(context.client, types);
		}
		lstate.batch_data->Append(chunk, lstate.partition_info.batch_index.GetIndex());
	} else {
		if (!lstate.collection) {
			lstate.collection = ChunkFormatOf(gstate).CreateCollection(context.client, types);
			lstate.collection->InitializeAppend(lstate.append_state);
		}
		lstate.collection->Append(lstate.append_state, chunk);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

unique_ptr<ResultUnit> PhysicalResultSink::AppendToUnit(ResultSinkGlobalState &gstate, ResultSinkLocalState &lstate,
                                                        DataChunk &chunk) const {
	auto &format = gstate.buffered_data->Format();
	auto &format_gstate = gstate.buffered_data->FormatState();
	auto &format_lstate = LocalFormatState(gstate, lstate);
	format.Append(format_gstate, format_lstate, chunk);
	return FinishUnit(gstate, lstate, false);
}

unique_ptr<ResultUnit> PhysicalResultSink::FinishUnit(ResultSinkGlobalState &gstate, ResultSinkLocalState &lstate,
                                                      bool flush_partial) const {
	if (!lstate.format_state) {
		return nullptr;
	}
	auto &format = gstate.buffered_data->Format();
	return format.Finish(gstate.buffered_data->FormatState(), *lstate.format_state, flush_partial);
}

SinkResultType PhysicalResultSink::SinkRetainedFormatted(ResultSinkGlobalState &gstate, ResultSinkLocalState &lstate,
                                                         DataChunk &chunk) const {
	if (BatchOrdered()) {
		lstate.current_batch = lstate.partition_info.batch_index.GetIndex();
	}
	if (auto unit = AppendToUnit(gstate, lstate, chunk)) {
		lstate.units.push_back({lstate.current_batch, std::move(unit)});
	}
	return SinkResultType::NEED_MORE_INPUT;
}

bool PhysicalResultSink::HandOver(ResultSinkGlobalState &gstate, ResultSinkLocalState &lstate,
                                  unique_ptr<ResultUnit> unit, const InterruptState &interrupt) const {
	if (BatchOrdered()) {
		return gstate.buffered_data->Cast<BatchedBufferedData>().AppendOrBlock(std::move(unit), lstate.current_batch,
		                                                                       interrupt);
	}
	return gstate.buffered_data->Cast<SimpleBufferedData>().AppendOrBlock(std::move(unit), interrupt);
}

SinkResultType PhysicalResultSink::SinkDraining(ResultSinkGlobalState &gstate, ResultSinkLocalState &lstate,
                                                DataChunk &chunk, OperatorSinkInput &input) const {
	if (lstate.chunk_deposited) {
		lstate.chunk_deposited = false;
		return SinkResultType::NEED_MORE_INPUT;
	}
	if (BatchOrdered()) {
		lstate.current_batch = lstate.partition_info.batch_index.GetIndex();
		gstate.buffered_data->Cast<BatchedBufferedData>().UpdateMinBatchIndex(
		    lstate.partition_info.min_batch_index.GetIndex());
	}
	auto unit = AppendToUnit(gstate, lstate, chunk);
	if (!unit) {
		return SinkResultType::NEED_MORE_INPUT;
	}
	if (HandOver(gstate, lstate, std::move(unit), input.interrupt_state)) {
		lstate.chunk_deposited = true;
		return SinkResultType::BLOCKED;
	}
	return SinkResultType::NEED_MORE_INPUT;
}

bool PhysicalResultSink::FlushPartialUnit(ResultSinkGlobalState &gstate, ResultSinkLocalState &lstate,
                                          const InterruptState &interrupt) const {
	auto unit = FinishUnit(gstate, lstate, true);
	if (!unit) {
		return false;
	}
	if (CurrentLifetime(gstate) == ResultLifetime::RETAINED) {
		lstate.units.push_back({lstate.current_batch, std::move(unit)});
		return false;
	}
	return HandOver(gstate, lstate, std::move(unit), interrupt);
}

SinkCombineResultType PhysicalResultSink::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<ResultSinkGlobalState>();
	auto &lstate = input.local_state.Cast<ResultSinkLocalState>();
	// A re-invocation finds no unit under construction: the first call moved it out with Finish
	if (FlushPartialUnit(gstate, lstate, input.interrupt_state)) {
		return SinkCombineResultType::BLOCKED;
	}
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
	if (!UsesChunkFormat(gstate)) {
		if (lstate.units.empty()) {
			return SinkCombineResultType::FINISHED;
		}
		annotated_lock_guard<annotated_mutex> l(gstate.glock);
		gstate.units.insert(gstate.units.end(), std::make_move_iterator(lstate.units.begin()),
		                    std::make_move_iterator(lstate.units.end()));
		lstate.units.clear();
		return SinkCombineResultType::FINISHED;
	}
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
	auto &lstate = input.local_state.Cast<ResultSinkLocalState>();
	// Finished before the producer moves on, so a unit never spans two batch indexes. A re-invocation
	// finds no unit under construction: the first call moved it out with Finish
	if (FlushPartialUnit(gstate, lstate, input.interrupt_state)) {
		return SinkNextBatchType::BLOCKED;
	}
	if (!DrainsByBatchIndex(gstate)) {
		return SinkNextBatchType::READY;
	}

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
	// A draining sink hands its units to the consumer through the buffer, never through a result
	D_ASSERT(CurrentLifetime(gstate) == ResultLifetime::RETAINED);
	return GetMaterializedResult(gstate);
}

unique_ptr<QueryResult> PhysicalResultSink::GetMaterializedResult(ResultSinkGlobalState &gstate) const {
	auto cc = gstate.context.lock();
	if (!cc) {
		throw ConnectionException("Connection has already been closed");
	}
	if (!UsesChunkFormat(gstate)) {
		return GetFormattedResult(gstate, *cc);
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
		collection = ChunkFormatOf(gstate).CreateCollection(*cc, types);
	}
	return make_uniq<QueryResult>(statement_type, properties, names, std::move(collection), cc->GetClientProperties());
}

unique_ptr<QueryResult> PhysicalResultSink::GetFormattedResult(ResultSinkGlobalState &gstate,
                                                               ClientContext &context) const {
	vector<RetainedUnit> retained;
	{
		annotated_lock_guard<annotated_mutex> l(gstate.glock);
		retained = std::move(gstate.units);
	}
	if (BatchOrdered()) {
		// Stable, so the units a producer contributed to one batch keep their production order
		std::stable_sort(retained.begin(), retained.end(),
		                 [](const RetainedUnit &lhs, const RetainedUnit &rhs) { return lhs.batch < rhs.batch; });
	}
	vector<unique_ptr<ResultUnit>> units;
	units.reserve(retained.size());
	for (auto &entry : retained) {
		units.push_back(std::move(entry.unit));
	}
	auto collection = make_uniq<ResultUnitCollection>(std::move(units));
	return make_uniq<QueryResult>(statement_type, properties, types, names, std::move(collection),
	                              gstate.buffered_data->SharedFormat(), gstate.buffered_data->SharedFormatState(),
	                              context.GetClientProperties());
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
