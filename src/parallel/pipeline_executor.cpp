#include "duckdb/parallel/pipeline_executor.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

PipelineExecutor::PipelineExecutor(ClientContext &context_p, Pipeline &pipeline_p) :
	pipeline(pipeline_p), thread(context_p), context(context_p, thread) {
	D_ASSERT(pipeline.source_state);
	RunFunctionInTryCatch([&]() {
		local_source_state = pipeline.source->GetLocalSourceState(context, *pipeline.source_state);
		if (pipeline.sink) {
			local_sink_state = pipeline.sink->GetLocalSinkState(context);
		}
		intermediate_chunks.reserve(pipeline.operators.size());
		intermediate_states.reserve(pipeline.operators.size());
		for(idx_t i = 0; i < pipeline.operators.size(); i++) {
			auto prev_operator = i == 0 ? pipeline.source : pipeline.operators[i - 1];
			auto chunk = make_unique<DataChunk>();
			chunk->Initialize(prev_operator->GetTypes());
			intermediate_chunks.push_back(move(chunk));
			intermediate_states.push_back(pipeline.operators[i]->GetOperatorState(context.client));
		}
		InitializeChunk(final_chunk);
	});
}

void PipelineExecutor::RunFunctionInTryCatch(const std::function<void(void)> &fun) {
	auto &executor = pipeline.executor;
	try {
		fun();
	} catch (Exception &ex) {
		executor.PushError(ex.type, ex.what());
	} catch (std::exception &ex) {
		executor.PushError(ExceptionType::UNKNOWN_TYPE, ex.what());
	} catch (...) { // LCOV_EXCL_START
		executor.PushError(ExceptionType::UNKNOWN_TYPE, "Unknown exception in Finalize!");
	} // LCOV_EXCL_STOP
}

void PipelineExecutor::Execute() {
	RunFunctionInTryCatch([&]() {
		D_ASSERT(pipeline.sink);
		auto &source_chunk = pipeline.operators.empty() ? final_chunk : *intermediate_chunks[0];
		while(true) {
			source_chunk.Reset();
			FetchFromSource(source_chunk);
			if (source_chunk.size() == 0) {
				break;
			}
			auto result = ExecutePushInternal(source_chunk);
			if (result == OperatorResultType::FINISHED) {
				break;
			}
		}
		PushFinalize();
	});
}

OperatorResultType PipelineExecutor::ExecutePush(DataChunk &input) {
	auto result = OperatorResultType::FINISHED;
	RunFunctionInTryCatch([&]() {
		result = ExecutePushInternal(input);
	});
	return result;
}

OperatorResultType PipelineExecutor::ExecutePushInternal(DataChunk &input) {
	D_ASSERT(pipeline.sink);
	if (input.size() == 0) {
		return OperatorResultType::NEED_MORE_INPUT;
	}
	while(true) {
		OperatorResultType result;
		if (!pipeline.operators.empty()) {
			final_chunk.Reset();
			result = Execute(input, final_chunk);
			if (result == OperatorResultType::FINISHED) {
				return OperatorResultType::FINISHED;
			}
		} else {
			result = OperatorResultType::NEED_MORE_INPUT;
		}
		auto &sink_chunk = pipeline.operators.empty() ? input : final_chunk;
		if (sink_chunk.size() > 0) {
			StartOperator(pipeline.sink);
			D_ASSERT(pipeline.sink);
			D_ASSERT(pipeline.sink->sink_state);
			auto sink_result = pipeline.sink->Sink(context, *pipeline.sink->sink_state, *local_sink_state, sink_chunk);
			EndOperator(pipeline.sink, nullptr);
			if (sink_result == SinkResultType::FINISHED) {
				return OperatorResultType::FINISHED;
			}
		}
		if (result == OperatorResultType::NEED_MORE_INPUT) {
			return OperatorResultType::NEED_MORE_INPUT;
		}
	}
	return OperatorResultType::FINISHED;
}

void PipelineExecutor::PushFinalize() {
	if (finalized) {
		throw InternalException("Calling PushFinalize on a pipeline that has been finalized already");
	}
	finalized = true;
	D_ASSERT(local_sink_state);
	pipeline.sink->Combine(context, *pipeline.sink->sink_state, *local_sink_state);
	pipeline.executor.Flush(thread);
	local_sink_state.reset();
}

void PipelineExecutor::ExecutePull(DataChunk &result) {
	auto &executor = pipeline.executor;
	try {
		D_ASSERT(!pipeline.sink);
		auto &source_chunk = pipeline.operators.empty() ? result : *intermediate_chunks[0];
		while(result.size() == 0) {
			if (in_process_operators.empty()) {
				source_chunk.Reset();
				FetchFromSource(source_chunk);
				if (source_chunk.size() == 0) {
					break;
				}
			}
			if (!pipeline.operators.empty()) {
				Execute(source_chunk, result);
			}
		}
	} catch (std::exception &ex) {
		if (executor.HasError()) {
			executor.ThrowException();
		}
		throw;
	} catch (...) { // LCOV_EXCL_START
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

void PipelineExecutor::GoToSource(idx_t &current_idx) {
	// we go back to the first operator (the source)
	current_idx = 0;
	if (!in_process_operators.empty()) {
		// ... UNLESS there is an in process operator
		// if there is an in-process operator, we start executing at the latest one
		// for example, if we have a join operator that has tuples left, we first need to emit those tuples
		current_idx = in_process_operators.top();
		in_process_operators.pop();
	}
}

OperatorResultType PipelineExecutor::Execute(DataChunk &input, DataChunk &result) {
	if (input.size() == 0) {
		return OperatorResultType::NEED_MORE_INPUT;
	}
	D_ASSERT(!pipeline.operators.empty());

	idx_t current_idx;
	GoToSource(current_idx);
	if (current_idx == 0) {
		current_idx++;
	}
	while(true) {
		if (context.client.interrupted) {
			throw InterruptException();
		}
		// now figure out where to put the chunk
		// if current_idx is the last possible index (>= operators.size()) we write to the result
		// otherwise we write to an intermediate chunk
		auto current_intermediate = current_idx;
		auto &current_chunk = current_intermediate >= intermediate_chunks.size() ? result : *intermediate_chunks[current_intermediate];
		current_chunk.Reset();
		if (current_idx == 0) {
			// we went back to the source: we need more input
			return OperatorResultType::NEED_MORE_INPUT;
		} else {
			auto current_operator = current_idx - 1;
			StartOperator(pipeline.operators[current_operator]);
			// if current_idx > source_idx, we pass the previous' operators output through the Execute of the current operator
			auto result = pipeline.operators[current_operator]->Execute(
				context,
				*intermediate_chunks[current_intermediate - 1],
				current_chunk,
				*intermediate_states[current_intermediate - 1]);
			EndOperator(pipeline.operators[current_operator], &current_chunk);
			if (result == OperatorResultType::HAVE_MORE_OUTPUT) {
				// more data remains in this operator
				// push in-process marker
				in_process_operators.push(current_idx);
			} else if (result == OperatorResultType::FINISHED) {
				D_ASSERT(current_chunk.size() == 0);
				return OperatorResultType::FINISHED;
			}
		}
		current_chunk.Verify();

		if (current_chunk.size() == 0) {
			// no output from this operator!
			if (current_idx == 0) {
				// if we got no output from the scan, we are done
				break;
			} else {
				// if we got no output from an intermediate op
				// we go back and try to pull data from the source again
				GoToSource(current_idx);
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

void PipelineExecutor::FetchFromSource(DataChunk &result) {
	StartOperator(pipeline.source);
	pipeline.source->GetData(
		context,
		result,
		*pipeline.source_state,
		*local_source_state);
	EndOperator(pipeline.source, &result);
}

void PipelineExecutor::InitializeChunk(DataChunk &chunk) {
	PhysicalOperator *last_op = pipeline.operators.empty() ? pipeline.source : pipeline.operators.back();
	chunk.Initialize(last_op->GetTypes());
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

}
