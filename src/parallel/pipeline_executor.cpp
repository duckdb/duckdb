#include "duckdb/parallel/pipeline_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/limits.hpp"

namespace duckdb {

PipelineExecutor::PipelineExecutor(ClientContext &context_p, Pipeline &pipeline_p)
    : pipeline(pipeline_p), thread(context_p), context(context_p, thread, &pipeline_p) {
	D_ASSERT(pipeline.source_state);
	local_source_state = pipeline.source->GetLocalSourceState(context, *pipeline.source_state);
	if (pipeline.sink) {
		local_sink_state = pipeline.sink->GetLocalSinkState(context);
		requires_batch_index = pipeline.sink->RequiresBatchIndex() && pipeline.source->SupportsBatchIndex();
	}

	intermediate_chunks.reserve(pipeline.operators.size());
	intermediate_states.reserve(pipeline.operators.size());
	for (idx_t i = 0; i < pipeline.operators.size(); i++) {
		auto prev_operator = i == 0 ? pipeline.source : pipeline.operators[i - 1];
		auto current_operator = pipeline.operators[i];

		auto chunk = make_uniq<DataChunk>();
		chunk->Initialize(Allocator::Get(context.client), prev_operator->GetTypes());
		intermediate_chunks.push_back(std::move(chunk));

		auto op_state = current_operator->GetOperatorState(context);
		intermediate_states.push_back(std::move(op_state));

		if (current_operator->IsSink() && current_operator->sink_state->state == SinkFinalizeType::NO_OUTPUT_POSSIBLE) {
			// one of the operators has already figured out no output is possible
			// we can skip executing the pipeline
			FinishProcessing();
		}
	}
	InitializeChunk(final_chunk);
}

OperatorResultType PipelineExecutor::FlushCachingOperatorsPush() {
	if (!started_flushing) {
		started_flushing = true;
		flushing_idx = IsFinished() ? idx_t(finished_processing_idx) : 0;
	}

	// Find next idx to flush
	while (flushing_idx < pipeline.operators.size()) {
		if (pipeline.operators[flushing_idx]->RequiresFinalExecute()) {
			break;
		}
		flushing_idx++;
	}

	// All operators are flushed
	if (flushing_idx >= pipeline.operators.size()){
		done_flushing = true;
		return OperatorResultType::FINISHED;
	}

	// TODO what if empty chunk?
	auto &curr_chunk =
		flushing_idx + 1 >= intermediate_chunks.size() ? final_chunk : *intermediate_chunks[flushing_idx + 1];
	auto current_operator = pipeline.operators[flushing_idx];
	StartOperator(current_operator);
	OperatorFinalizeResultType finalize_result = current_operator->FinalExecute(context, curr_chunk, *current_operator->op_state,
																				*intermediate_states[flushing_idx]);
	EndOperator(current_operator, &curr_chunk);

	// Push chunk into pipeline
	auto push_result = ExecutePushInternal(curr_chunk, flushing_idx + 1);

	// We're done flushing this operator, we can move on to the next one next time;
	if (finalize_result == OperatorFinalizeResultType::FINISHED) {
		flushing_idx++;
	}

	// Sink interrupted -> when continueing the pipeline we need to re-sink the final_chunk
	if (push_result == OperatorResultType::BLOCKED) {
		return OperatorResultType::BLOCKED;
	}

	// Sink is done!
	if (push_result == OperatorResultType::FINISHED) {
		done_flushing = true;
		return OperatorResultType::FINISHED;
	}

	return OperatorResultType::NEED_MORE_INPUT;
}

PipelineExecuteResult PipelineExecutor::Execute(idx_t max_chunks) {
	D_ASSERT(pipeline.sink);
	bool exhausted_source = false;
	auto &source_chunk = pipeline.operators.empty() ? final_chunk : *intermediate_chunks[0];
	for (idx_t i = 0; i < max_chunks; i++) {
		if (exhausted_source && done_flushing && !remaining_sink_chunk) {
			break;
		}

		// There's 4 ways we can process a chunk in the pipeline here:
		// 1. Regular fetch from source into pipeline:      Fetch from source, push through pipeline
		// 2. Resuming after Sink interrupt:                Retry pushing the final chunk into the sink
		// 3. Flushing operators:                           Flushing a cached chunk through the pipeline into sink
		// 4. Resume in process ops after Sink interrupt:   Retry pushing the last source chunk through the pipeline
		OperatorResultType result;
		if (remaining_sink_chunk) {
			// Resuming after Sink interrupt
			result = ExecutePushInternal(final_chunk);
			remaining_sink_chunk = false;
		} else if (!in_process_operators.empty()) {
			// Resume in process ops after Sink interrupt
			result = ExecutePushInternal(source_chunk);
		} else if (exhausted_source && !done_flushing) {
			// Flushing operators
			result = FlushCachingOperatorsPush();
		} else if (!exhausted_source) {
			// Regular fetch from source into pipeline
			source_chunk.Reset();
			FetchFromSource(source_chunk, true);

			// SOURCE INTERRUPT
			if(interrupt_state.result != InterruptResultType::NO_INTERRUPT) {
				return PipelineExecuteResult::INTERRUPTED;
			}

			if (source_chunk.size() == 0) {
				exhausted_source = true;
				continue;
			}
			result = ExecutePushInternal(source_chunk);
		} else {
			throw InternalException("Unexpected state reached in pipeline executor");
		}

		// SINK INTERRUPT
		if(result == OperatorResultType::BLOCKED) {
			D_ASSERT(interrupt_state.result != InterruptResultType::NO_INTERRUPT);
			remaining_sink_chunk = true;
			return PipelineExecuteResult::INTERRUPTED;
		}

		if (result == OperatorResultType::FINISHED) {
			break;
		}
	}

	// TODO this may be wrong?
	if (!exhausted_source && !IsFinished()) {
		return PipelineExecuteResult::NOT_FINISHED;
	}

	PushFinalize();

	return PipelineExecuteResult::FINISHED;
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
			StartOperator(pipeline.sink);
			D_ASSERT(pipeline.sink);
			D_ASSERT(pipeline.sink->sink_state);

			interrupt_state.Reset();
			auto sink_result = pipeline.sink->Sink(context, *pipeline.sink->sink_state, *local_sink_state, sink_chunk, interrupt_state);

			EndOperator(pipeline.sink, nullptr);

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

void PipelineExecutor::PushFinalize() {
	if (finalized) {
		throw InternalException("Calling PushFinalize on a pipeline that has been finalized already");
	}

	D_ASSERT(local_sink_state);

	finalized = true;

	// run the combine for the sink
	pipeline.sink->Combine(context, *pipeline.sink->sink_state, *local_sink_state);

	// flush all query profiler info
	for (idx_t i = 0; i < intermediate_states.size(); i++) {
		intermediate_states[i]->Finalize(pipeline.operators[i], context);
	}
	pipeline.executor.Flush(thread);
	local_sink_state.reset();
}

void PipelineExecutor::ExecutePull(DataChunk &result) {
	if (IsFinished()) {
		return;
	}
	auto &executor = pipeline.executor;
	try {
		D_ASSERT(!pipeline.sink);
		auto &source_chunk = pipeline.operators.empty() ? result : *intermediate_chunks[0];
		while (result.size() == 0) {
			if (in_process_operators.empty()) {
				source_chunk.Reset();

				// TODO: here we currently force having a sync and an async implementation initially, we may want to
				//		 switch to a busy wait for the async variant so both are fine?
				FetchFromSource(source_chunk, false);

				if (source_chunk.size() == 0) {
					break;
				}
			}
			if (!pipeline.operators.empty()) {
				auto state = Execute(source_chunk, result);
				if (state == OperatorResultType::FINISHED) {
					break;
				}
			}
		}
	} catch (const Exception &ex) { // LCOV_EXCL_START
		if (executor.HasError()) {
			executor.ThrowException();
		}
		throw;
	} catch (std::exception &ex) {
		if (executor.HasError()) {
			executor.ThrowException();
		}
		throw;
	} catch (...) {
		if (executor.HasError()) {
			executor.ThrowException();
		}
		throw;
	} // LCOV_EXCL_STOP
}

void PipelineExecutor::PullFinalize() {
	if (finalized) {
		throw InternalException("Calling PullFinalize on a pipeline that has been finalized already");
	}
	finalized = true;
	pipeline.executor.Flush(thread);
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
			auto current_operator = pipeline.operators[operator_idx];

			// if current_idx > source_idx, we pass the previous' operators output through the Execute of the current
			// operator
			StartOperator(current_operator);
			auto result = current_operator->Execute(context, prev_chunk, current_chunk, *current_operator->op_state,
			                                        *intermediate_states[current_intermediate - 1]);
			EndOperator(current_operator, &current_chunk);
			if (result == OperatorResultType::HAVE_MORE_OUTPUT) {
				// more data remains in this operator
				// push in-process marker
				in_process_operators.push(current_idx);
			} else if (result == OperatorResultType::FINISHED) {
				D_ASSERT(current_chunk.size() == 0);
				FinishProcessing(current_idx);
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

void PipelineExecutor::FetchFromSource(DataChunk &result, bool allow_async) {
	StartOperator(pipeline.source);

	interrupt_state.Reset(); // TODO Do we need to reset this every time? or can we actually guarantee its reset here?

	if (allow_async) {
		pipeline.source->GetData(context, result, *pipeline.source_state, *local_source_state, interrupt_state);

		// Safety check that we don't interrupt + return data;
		D_ASSERT(interrupt_state.result == InterruptResultType::NO_INTERRUPT || result.size() == 0);
	} else {
		pipeline.source->GetData(context, result, *pipeline.source_state, *local_source_state);
	}

	if (result.size() != 0 && requires_batch_index) {
		auto next_batch_index =
		    pipeline.source->GetBatchIndex(context, result, *pipeline.source_state, *local_source_state);
		next_batch_index += pipeline.base_batch_index;
		D_ASSERT(local_sink_state->batch_index <= next_batch_index ||
		         local_sink_state->batch_index == DConstants::INVALID_INDEX);
		local_sink_state->batch_index = next_batch_index;
	}

	// TODO: how to handle the profiler when a pipeline was interrupted?
	EndOperator(pipeline.source, &result);
}

void PipelineExecutor::InitializeChunk(DataChunk &chunk) {
	PhysicalOperator *last_op = pipeline.operators.empty() ? pipeline.source : pipeline.operators.back();
	chunk.Initialize(Allocator::DefaultAllocator(), last_op->GetTypes());
}

void PipelineExecutor::StartOperator(PhysicalOperator *op) {
	if (context.client.interrupted) {
		throw InterruptException();
	}
	context.thread.profiler.StartOperator(op);
}

void PipelineExecutor::EndOperator(PhysicalOperator *op, DataChunk *chunk) {
	context.thread.profiler.EndOperator(chunk);

	if (chunk) {
		chunk->Verify();
	}
}

} // namespace duckdb
