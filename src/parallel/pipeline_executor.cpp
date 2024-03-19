#include "duckdb/parallel/pipeline_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/limits.hpp"

#ifdef DUCKDB_DEBUG_ASYNC_SINK_SOURCE
#include <thread>
#include <chrono>
#endif

namespace duckdb {

PipelineExecutor::PipelineExecutor(ClientContext &context_p, Pipeline &pipeline_p)
    : pipeline(pipeline_p), thread(context_p), context(context_p, thread, &pipeline_p) {
	D_ASSERT(pipeline.source_state);
	if (pipeline.sink) {
		local_sink_state = pipeline.sink->GetLocalSinkState(context);
		requires_batch_index = pipeline.sink->RequiresBatchIndex() && pipeline.source->SupportsBatchIndex();
		if (requires_batch_index) {
			auto &partition_info = local_sink_state->partition_info;
			D_ASSERT(!partition_info.batch_index.IsValid());
			// batch index is not set yet - initialize before fetching anything
			partition_info.batch_index = pipeline.RegisterNewBatchIndex();
			partition_info.min_batch_index = partition_info.batch_index;
		}
	}
	local_source_state = pipeline.source->GetLocalSourceState(context, *pipeline.source_state);

	intermediate_chunks.reserve(pipeline.operators.size());
	intermediate_states.reserve(pipeline.operators.size());
	for (idx_t i = 0; i < pipeline.operators.size(); i++) {
		auto &prev_operator = i == 0 ? *pipeline.source : pipeline.operators[i - 1].get();
		auto &current_operator = pipeline.operators[i].get();

		auto chunk = make_uniq<DataChunk>();
		chunk->Initialize(Allocator::Get(context.client), prev_operator.GetTypes());
		intermediate_chunks.push_back(std::move(chunk));

		auto op_state = current_operator.GetOperatorState(context);
		intermediate_states.push_back(std::move(op_state));

		if (current_operator.IsSink() && current_operator.sink_state->state == SinkFinalizeType::NO_OUTPUT_POSSIBLE) {
			// one of the operators has already figured out no output is possible
			// we can skip executing the pipeline
			FinishProcessing();
		}
	}
	InitializeChunk(final_chunk);
}

bool PipelineExecutor::TryFlushCachingOperators() {
	if (!started_flushing) {
		// Remainder of this method assumes any in process operators are from flushing
		D_ASSERT(in_process_operators.empty());
		started_flushing = true;
		flushing_idx = IsFinished() ? idx_t(finished_processing_idx) : 0;
	}

	// Go over each operator and keep flushing them using `FinalExecute` until empty
	while (flushing_idx < pipeline.operators.size()) {
		if (!pipeline.operators[flushing_idx].get().RequiresFinalExecute()) {
			flushing_idx++;
			continue;
		}

		// This slightly awkward way of increasing the flushing idx is to make the code re-entrant: We need to call this
		// method again in the case of a Sink returning BLOCKED.
		if (!should_flush_current_idx && in_process_operators.empty()) {
			should_flush_current_idx = true;
			flushing_idx++;
			continue;
		}

		auto &curr_chunk =
		    flushing_idx + 1 >= intermediate_chunks.size() ? final_chunk : *intermediate_chunks[flushing_idx + 1];
		auto &current_operator = pipeline.operators[flushing_idx].get();

		OperatorFinalizeResultType finalize_result;
		OperatorResultType push_result;

		if (in_process_operators.empty()) {
			curr_chunk.Reset();
			StartOperator(current_operator);
			finalize_result = current_operator.FinalExecute(context, curr_chunk, *current_operator.op_state,
			                                                *intermediate_states[flushing_idx]);
			EndOperator(current_operator, &curr_chunk);
		} else {
			// Reset flag and reflush the last chunk we were flushing.
			finalize_result = OperatorFinalizeResultType::HAVE_MORE_OUTPUT;
		}

		push_result = ExecutePushInternal(curr_chunk, flushing_idx + 1);

		if (finalize_result == OperatorFinalizeResultType::HAVE_MORE_OUTPUT) {
			should_flush_current_idx = true;
		} else {
			should_flush_current_idx = false;
		}

		if (push_result == OperatorResultType::BLOCKED) {
			remaining_sink_chunk = true;
			return false;
		} else if (push_result == OperatorResultType::FINISHED) {
			break;
		}
	}
	return true;
}

SinkNextBatchType PipelineExecutor::NextBatch(duckdb::DataChunk &source_chunk) {
	D_ASSERT(requires_batch_index);
	idx_t next_batch_index;
	auto max_batch_index = pipeline.base_batch_index + PipelineBuildState::BATCH_INCREMENT - 1;
	if (source_chunk.size() == 0) {
		// set it to the maximum valid batch index value for the current pipeline
		next_batch_index = max_batch_index;
	} else {
		auto batch_index =
		    pipeline.source->GetBatchIndex(context, source_chunk, *pipeline.source_state, *local_source_state);
		// we start with the base_batch_index as a valid starting value. Make sure that next batch is called below
		next_batch_index = pipeline.base_batch_index + batch_index + 1;
		if (next_batch_index >= max_batch_index) {
			throw InternalException("Pipeline batch index - invalid batch index %llu returned by source operator",
			                        batch_index);
		}
	}
	auto &partition_info = local_sink_state->partition_info;
	if (next_batch_index == partition_info.batch_index.GetIndex()) {
		// no changes, return
		return SinkNextBatchType::READY;
	}
	// batch index has changed - update it
	if (partition_info.batch_index.GetIndex() > next_batch_index) {
		throw InternalException(
		    "Pipeline batch index - gotten lower batch index %llu (down from previous batch index of %llu)",
		    next_batch_index, partition_info.batch_index.GetIndex());
	}
#ifdef DUCKDB_DEBUG_ASYNC_SINK_SOURCE
	if (debug_blocked_next_batch_count < debug_blocked_target_count) {
		debug_blocked_next_batch_count++;

		auto &callback_state = interrupt_state;
		std::thread rewake_thread([callback_state] {
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
			callback_state.Callback();
		});
		rewake_thread.detach();

		return SinkNextBatchType::BLOCKED;
	}
#endif
	auto current_batch = partition_info.batch_index.GetIndex();
	partition_info.batch_index = next_batch_index;
	OperatorSinkNextBatchInput next_batch_input {*pipeline.sink->sink_state, *local_sink_state, interrupt_state};
	// call NextBatch before updating min_batch_index to provide the opportunity to flush the previous batch
	auto next_batch_result = pipeline.sink->NextBatch(context, next_batch_input);

	if (next_batch_result == SinkNextBatchType::BLOCKED) {
		partition_info.batch_index = current_batch; // set batch_index back to what it was before
		return SinkNextBatchType::BLOCKED;
	}

	partition_info.min_batch_index = pipeline.UpdateBatchIndex(current_batch, next_batch_index);

	return SinkNextBatchType::READY;
}

PipelineExecuteResult PipelineExecutor::Execute(idx_t max_chunks) {
	D_ASSERT(pipeline.sink);
	auto &source_chunk = pipeline.operators.empty() ? final_chunk : *intermediate_chunks[0];
	for (idx_t i = 0; i < max_chunks; i++) {
		if (context.client.interrupted) {
			throw InterruptException();
		}

		OperatorResultType result;
		if (exhausted_source && done_flushing && !remaining_sink_chunk && !next_batch_blocked &&
		    in_process_operators.empty()) {
			break;
		} else if (remaining_sink_chunk) {
			// The pipeline was interrupted by the Sink. We should retry sinking the final chunk.
			result = ExecutePushInternal(final_chunk);
			remaining_sink_chunk = false;
		} else if (!in_process_operators.empty() && !started_flushing) {
			// The pipeline was interrupted by the Sink when pushing a source chunk through the pipeline. We need to
			// re-push the same source chunk through the pipeline because there are in_process operators, meaning that
			// the result for the pipeline
			D_ASSERT(source_chunk.size() > 0);
			result = ExecutePushInternal(source_chunk);
		} else if (exhausted_source && !next_batch_blocked && !done_flushing) {
			// The source was exhausted, try flushing all operators
			auto flush_completed = TryFlushCachingOperators();
			if (flush_completed) {
				done_flushing = true;
				break;
			} else {
				return PipelineExecuteResult::INTERRUPTED;
			}
		} else if (!exhausted_source || next_batch_blocked) {
			SourceResultType source_result;
			if (!next_batch_blocked) {
				// "Regular" path: fetch a chunk from the source and push it through the pipeline
				source_chunk.Reset();
				source_result = FetchFromSource(source_chunk);
				if (source_result == SourceResultType::BLOCKED) {
					return PipelineExecuteResult::INTERRUPTED;
				}
				if (source_result == SourceResultType::FINISHED) {
					exhausted_source = true;
				}
			}

			if (requires_batch_index) {
				auto next_batch_result = NextBatch(source_chunk);
				next_batch_blocked = next_batch_result == SinkNextBatchType::BLOCKED;
				if (next_batch_blocked) {
					return PipelineExecuteResult::INTERRUPTED;
				}
			}

			if (exhausted_source && source_chunk.size() == 0) {
				// To ensure that we're not early-terminating the pipeline
				continue;
			}

			result = ExecutePushInternal(source_chunk);
		} else {
			throw InternalException("Unexpected state reached in pipeline executor");
		}

		// SINK INTERRUPT
		if (result == OperatorResultType::BLOCKED) {
			remaining_sink_chunk = true;
			return PipelineExecuteResult::INTERRUPTED;
		}

		if (result == OperatorResultType::FINISHED) {
			break;
		}
	}

	if ((!exhausted_source || !done_flushing) && !IsFinished()) {
		return PipelineExecuteResult::NOT_FINISHED;
	}

	return PushFinalize();
}

bool PipelineExecutor::RemainingSinkChunk() const {
	return remaining_sink_chunk;
}

PipelineExecuteResult PipelineExecutor::Execute() {
	return Execute(NumericLimits<idx_t>::Maximum());
}

OperatorResultType PipelineExecutor::ExecutePush(DataChunk &input) { // LCOV_EXCL_START
	return ExecutePushInternal(input);
} // LCOV_EXCL_STOP

void PipelineExecutor::FinishProcessing(int32_t operator_idx) {
	finished_processing_idx = operator_idx < 0 ? NumericLimits<int32_t>::Maximum() : operator_idx;
	in_process_operators = stack<idx_t>();
}

bool PipelineExecutor::IsFinished() {
	return finished_processing_idx >= 0;
}

OperatorResultType PipelineExecutor::ExecutePushInternal(DataChunk &input, idx_t initial_idx) {
	D_ASSERT(pipeline.sink);
	if (input.size() == 0) { // LCOV_EXCL_START
		return OperatorResultType::NEED_MORE_INPUT;
	} // LCOV_EXCL_STOP

	// this loop will continuously push the input chunk through the pipeline as long as:
	// - the OperatorResultType for the Execute is HAVE_MORE_OUTPUT
	// - the Sink doesn't block
	while (true) {
		OperatorResultType result;
		// Note: if input is the final_chunk, we don't do any executing, the chunk just needs to be sinked
		if (&input != &final_chunk) {
			final_chunk.Reset();
			result = Execute(input, final_chunk, initial_idx);
			if (result == OperatorResultType::FINISHED) {
				return OperatorResultType::FINISHED;
			}
		} else {
			result = OperatorResultType::NEED_MORE_INPUT;
		}
		auto &sink_chunk = final_chunk;
		if (sink_chunk.size() > 0) {
			StartOperator(*pipeline.sink);
			D_ASSERT(pipeline.sink);
			D_ASSERT(pipeline.sink->sink_state);
			OperatorSinkInput sink_input {*pipeline.sink->sink_state, *local_sink_state, interrupt_state};

			auto sink_result = Sink(sink_chunk, sink_input);

			EndOperator(*pipeline.sink, nullptr);

			if (sink_result == SinkResultType::BLOCKED) {
				return OperatorResultType::BLOCKED;
			} else if (sink_result == SinkResultType::FINISHED) {
				FinishProcessing();
				return OperatorResultType::FINISHED;
			}
		}
		if (result == OperatorResultType::NEED_MORE_INPUT) {
			return OperatorResultType::NEED_MORE_INPUT;
		}
	}
}

PipelineExecuteResult PipelineExecutor::PushFinalize() {
	if (finalized) {
		throw InternalException("Calling PushFinalize on a pipeline that has been finalized already");
	}

	D_ASSERT(local_sink_state);

	// Run the combine for the sink
	OperatorSinkCombineInput combine_input {*pipeline.sink->sink_state, *local_sink_state, interrupt_state};

#ifdef DUCKDB_DEBUG_ASYNC_SINK_SOURCE
	if (debug_blocked_combine_count < debug_blocked_target_count) {
		debug_blocked_combine_count++;

		auto &callback_state = combine_input.interrupt_state;
		std::thread rewake_thread([callback_state] {
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
			callback_state.Callback();
		});
		rewake_thread.detach();

		return PipelineExecuteResult::INTERRUPTED;
	}
#endif
	auto result = pipeline.sink->Combine(context, combine_input);

	if (result == SinkCombineResultType::BLOCKED) {
		return PipelineExecuteResult::INTERRUPTED;
	}

	finalized = true;
	// flush all query profiler info
	for (idx_t i = 0; i < intermediate_states.size(); i++) {
		intermediate_states[i]->Finalize(pipeline.operators[i].get(), context);
	}
	pipeline.executor.Flush(thread);
	local_sink_state.reset();

	return PipelineExecuteResult::FINISHED;
}

void PipelineExecutor::GoToSource(idx_t &current_idx, idx_t initial_idx) {
	// we go back to the first operator (the source)
	current_idx = initial_idx;
	if (!in_process_operators.empty()) {
		// ... UNLESS there is an in process operator
		// if there is an in-process operator, we start executing at the latest one
		// for example, if we have a join operator that has tuples left, we first need to emit those tuples
		current_idx = in_process_operators.top();
		in_process_operators.pop();
	}
	D_ASSERT(current_idx >= initial_idx);
}

OperatorResultType PipelineExecutor::Execute(DataChunk &input, DataChunk &result, idx_t initial_idx) {
	if (input.size() == 0) { // LCOV_EXCL_START
		return OperatorResultType::NEED_MORE_INPUT;
	} // LCOV_EXCL_STOP
	D_ASSERT(!pipeline.operators.empty());

	idx_t current_idx;
	GoToSource(current_idx, initial_idx);
	if (current_idx == initial_idx) {
		current_idx++;
	}
	if (current_idx > pipeline.operators.size()) {
		result.Reference(input);
		return OperatorResultType::NEED_MORE_INPUT;
	}
	while (true) {
		if (context.client.interrupted) {
			throw InterruptException();
		}
		// now figure out where to put the chunk
		// if current_idx is the last possible index (>= operators.size()) we write to the result
		// otherwise we write to an intermediate chunk
		auto current_intermediate = current_idx;
		auto &current_chunk =
		    current_intermediate >= intermediate_chunks.size() ? result : *intermediate_chunks[current_intermediate];
		current_chunk.Reset();
		if (current_idx == initial_idx) {
			// we went back to the source: we need more input
			return OperatorResultType::NEED_MORE_INPUT;
		} else {
			auto &prev_chunk =
			    current_intermediate == initial_idx + 1 ? input : *intermediate_chunks[current_intermediate - 1];
			auto operator_idx = current_idx - 1;
			auto &current_operator = pipeline.operators[operator_idx].get();

			// if current_idx > source_idx, we pass the previous operators' output through the Execute of the current
			// operator
			StartOperator(current_operator);
			auto result = current_operator.Execute(context, prev_chunk, current_chunk, *current_operator.op_state,
			                                       *intermediate_states[current_intermediate - 1]);
			EndOperator(current_operator, &current_chunk);
			if (result == OperatorResultType::HAVE_MORE_OUTPUT) {
				// more data remains in this operator
				// push in-process marker
				in_process_operators.push(current_idx);
			} else if (result == OperatorResultType::FINISHED) {
				D_ASSERT(current_chunk.size() == 0);
				FinishProcessing(NumericCast<int32_t>(current_idx));
				return OperatorResultType::FINISHED;
			}
			current_chunk.Verify();
		}

		if (current_chunk.size() == 0) {
			// no output from this operator!
			if (current_idx == initial_idx) {
				// if we got no output from the scan, we are done
				break;
			} else {
				// if we got no output from an intermediate op
				// we go back and try to pull data from the source again
				GoToSource(current_idx, initial_idx);
				continue;
			}
		} else {
			// we got output! continue to the next operator
			current_idx++;
			if (current_idx > pipeline.operators.size()) {
				// if we got output and are at the last operator, we are finished executing for this output chunk
				// return the data and push it into the chunk
				break;
			}
		}
	}
	return in_process_operators.empty() ? OperatorResultType::NEED_MORE_INPUT : OperatorResultType::HAVE_MORE_OUTPUT;
}

void PipelineExecutor::SetTaskForInterrupts(weak_ptr<Task> current_task) {
	interrupt_state = InterruptState(std::move(current_task));
}

SourceResultType PipelineExecutor::GetData(DataChunk &chunk, OperatorSourceInput &input) {
	//! Testing feature to enable async source on every operator
#ifdef DUCKDB_DEBUG_ASYNC_SINK_SOURCE
	if (debug_blocked_source_count < debug_blocked_target_count) {
		debug_blocked_source_count++;

		auto &callback_state = input.interrupt_state;
		std::thread rewake_thread([callback_state] {
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
			callback_state.Callback();
		});
		rewake_thread.detach();

		return SourceResultType::BLOCKED;
	}
#endif

	return pipeline.source->GetData(context, chunk, input);
}

SinkResultType PipelineExecutor::Sink(DataChunk &chunk, OperatorSinkInput &input) {
	//! Testing feature to enable async sink on every operator
#ifdef DUCKDB_DEBUG_ASYNC_SINK_SOURCE
	if (debug_blocked_sink_count < debug_blocked_target_count) {
		debug_blocked_sink_count++;

		auto &callback_state = input.interrupt_state;
		std::thread rewake_thread([callback_state] {
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
			callback_state.Callback();
		});
		rewake_thread.detach();

		return SinkResultType::BLOCKED;
	}
#endif
	return pipeline.sink->Sink(context, chunk, input);
}

SourceResultType PipelineExecutor::FetchFromSource(DataChunk &result) {
	StartOperator(*pipeline.source);

	OperatorSourceInput source_input = {*pipeline.source_state, *local_source_state, interrupt_state};
	auto res = GetData(result, source_input);

	// Ensures Sinks only return empty results when Blocking or Finished
	D_ASSERT(res != SourceResultType::BLOCKED || result.size() == 0);

	EndOperator(*pipeline.source, &result);

	return res;
}

void PipelineExecutor::InitializeChunk(DataChunk &chunk) {
	auto &last_op = pipeline.operators.empty() ? *pipeline.source : pipeline.operators.back().get();
	chunk.Initialize(Allocator::DefaultAllocator(), last_op.GetTypes());
}

void PipelineExecutor::StartOperator(PhysicalOperator &op) {
	if (context.client.interrupted) {
		throw InterruptException();
	}
	context.thread.profiler.StartOperator(&op);
}

void PipelineExecutor::EndOperator(PhysicalOperator &op, optional_ptr<DataChunk> chunk) {
	context.thread.profiler.EndOperator(chunk);

	if (chunk) {
		chunk->Verify();
	}
}

} // namespace duckdb
