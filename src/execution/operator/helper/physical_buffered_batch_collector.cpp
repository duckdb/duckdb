#include "duckdb/execution/operator/helper/physical_buffered_batch_collector.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/main/buffered_data/batched_buffered_data.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/stream_query_result.hpp"

namespace duckdb {

PhysicalBufferedBatchCollector::PhysicalBufferedBatchCollector(PhysicalPlan &physical_plan, PreparedStatementData &data)
    : PhysicalResultCollector(physical_plan, data) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class BufferedBatchCollectorGlobalState : public GlobalSinkState {
public:
	explicit BufferedBatchCollectorGlobalState(ClientContext &context)
	    : buffered_data(make_shared_ptr<BatchedBufferedData>(context)) {
	}

	shared_ptr<BatchedBufferedData> buffered_data;
};

SinkResultType PhysicalBufferedBatchCollector::Sink(ExecutionContext &context, DataChunk &chunk,
                                                    OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<BufferedBatchCollectorGlobalState>();
	auto &lstate = input.local_state.Cast<BufferedBatchCollectorLocalState>();

	lstate.current_batch = lstate.partition_info.batch_index.GetIndex();
	auto batch = lstate.partition_info.batch_index.GetIndex();
	auto min_batch_index = lstate.partition_info.min_batch_index.GetIndex();

	auto &buffered_data = *gstate.buffered_data;
	buffered_data.UpdateMinBatchIndex(min_batch_index);

	if (buffered_data.ShouldBlockBatch(batch)) {
		buffered_data.BlockSink(input.interrupt_state, batch);
		return SinkResultType::BLOCKED;
	}

	// FIXME: if we want to make this more accurate, we should grab a reservation on the buffer space
	// while we're unlocked some other thread could also append, causing us to potentially cross our buffer size

	buffered_data.Append(chunk, batch);

	return SinkResultType::NEED_MORE_INPUT;
}

SinkNextBatchType PhysicalBufferedBatchCollector::NextBatch(ExecutionContext &context,
                                                            OperatorSinkNextBatchInput &input) const {
	auto &gstate = input.global_state.Cast<BufferedBatchCollectorGlobalState>();
	auto &lstate = input.local_state.Cast<BufferedBatchCollectorLocalState>();

	auto batch = lstate.current_batch;
	auto min_batch_index = lstate.partition_info.min_batch_index.GetIndex();
	auto new_index = lstate.partition_info.batch_index.GetIndex();

	auto &buffered_data = *gstate.buffered_data;
	buffered_data.CompleteBatch(batch);
	lstate.current_batch = new_index;
	// FIXME: this can move from the buffer to the read queue, increasing the 'read_queue_byte_count'
	// We might want to block here if 'read_queue_byte_count' has already reached the ReadQueueCapacity()
	// So we don't completely disregard the 'streaming_buffer_size' that was set
	buffered_data.UpdateMinBatchIndex(min_batch_index);
	return SinkNextBatchType::READY;
}

SinkNextBatchType PhysicalBufferedBatchCollector::UpdateMinBatchIndex(ExecutionContext &,
                                                                      OperatorSinkNextBatchInput &input) const {
	auto &gstate = input.global_state.Cast<BufferedBatchCollectorGlobalState>();
	auto min_batch_index = input.local_state.partition_info.min_batch_index.GetIndex();
	gstate.buffered_data->UpdateMinBatchIndex(min_batch_index);
	return SinkNextBatchType::READY;
}

SinkCombineResultType PhysicalBufferedBatchCollector::Combine(ExecutionContext &context,
                                                              OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<BufferedBatchCollectorGlobalState>();
	auto &lstate = input.local_state.Cast<BufferedBatchCollectorLocalState>();

	auto min_batch_index = lstate.partition_info.min_batch_index.GetIndex();
	auto &buffered_data = *gstate.buffered_data;

	// FIXME: this can move from the buffer to the read queue, increasing the 'read_queue_byte_count'
	// We might want to block here if 'read_queue_byte_count' has already reached the ReadQueueCapacity()
	// So we don't completely disregard the 'streaming_buffer_size' that was set
	buffered_data.UpdateMinBatchIndex(min_batch_index);
	return SinkCombineResultType::FINISHED;
}

unique_ptr<LocalSinkState> PhysicalBufferedBatchCollector::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<BufferedBatchCollectorLocalState>();
}

unique_ptr<GlobalSinkState> PhysicalBufferedBatchCollector::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<BufferedBatchCollectorGlobalState>(context);
}

unique_ptr<QueryResult> PhysicalBufferedBatchCollector::GetResult(GlobalSinkState &state) const {
	auto &gstate = state.Cast<BufferedBatchCollectorGlobalState>();
	auto context = gstate.buffered_data->GetContext();
	if (!context) {
		throw ConnectionException("Connection has already been closed");
	}
	return make_uniq<StreamQueryResult>(statement_type, properties, types, names, context->GetClientProperties(),
	                                    gstate.buffered_data);
}

} // namespace duckdb
