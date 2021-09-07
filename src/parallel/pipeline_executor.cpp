#include "duckdb/parallel/pipeline_executor.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

PipelineExecutor::PipelineExecutor(ClientContext &context_p, Pipeline &pipeline_p) :
	pipeline(pipeline_p), thread(context_p), context(context_p, thread) {
	D_ASSERT(pipeline.source_state);
	local_source_state = pipeline.operators[pipeline.source_idx]->GetLocalSourceState(context, *pipeline.source_state);
	if (pipeline.sink) {
		local_sink_state = pipeline.sink->GetLocalSinkState(context);
	}
	intermediate_chunks.reserve(pipeline.operators.size());
	intermediate_states.reserve(pipeline.operators.size());
	for(idx_t i = pipeline.source_idx + 1; i < pipeline.operators.size(); i++) {
		auto prev_operator = pipeline.operators[i - 1];
		auto chunk = make_unique<DataChunk>();
		chunk->Initialize(prev_operator->GetTypes());
		intermediate_chunks.push_back(move(chunk));
		intermediate_states.push_back(pipeline.operators[i]->GetOperatorState(context.client));
	}
}

void PipelineExecutor::Execute() {
	D_ASSERT(pipeline.sink);
	if (context.client.interrupted) {
		return;
	}

	DataChunk final_chunk;
	InitializeChunk(final_chunk);
	auto &executor = pipeline.executor;
	try {
		while(true) {
			final_chunk.Reset();
			Execute(final_chunk);
			if (final_chunk.size() == 0) {
				pipeline.sink->Combine(context, *pipeline.sink->sink_state, *local_sink_state);
				break;
			}
			StartOperator(pipeline.sink);
			pipeline.sink->Sink(context, *pipeline.sink->sink_state, *local_sink_state, final_chunk);
			EndOperator(pipeline.sink, nullptr);
		}
	} catch (std::exception &ex) {
		executor.PushError(ex.what());
	} catch (...) { // LCOV_EXCL_START
		executor.PushError("Unknown exception in pipeline!");
	} // LCOV_EXCL_STOP
	executor.Flush(thread);
}

void PipelineExecutor::GoToSource(idx_t &current_idx) {
	// we go back to the first operator (the source)
	current_idx = pipeline.source_idx;
	if (!in_process_operators.empty()) {
		// ... UNLESS there is an in process operator
		// if there is an in-process operator, we start executing at the latest one
		// for example, if we have a join operator that has tuples left, we first need to emit those tuples
		current_idx = in_process_operators.top();
		in_process_operators.pop();
	}
}

void PipelineExecutor::Execute(DataChunk &result) {
	if (context.client.interrupted) {
		return;
	}

	idx_t current_idx;
	GoToSource(current_idx);
	while(true) {
		// now figure out where to put the chunk
		// if current_idx is the last possible index (>= operators.size()) we write to the result
		// otherwise we write to an intermediate chunk
		auto current_intermediate = current_idx - pipeline.source_idx;
		auto &current_chunk = current_idx >= intermediate_chunks.size() ? result : *intermediate_chunks[current_intermediate];
		current_chunk.Reset();
		StartOperator(pipeline.operators[current_idx]);
		if (current_idx == pipeline.source_idx) {
			// if current_idx = 0 we fetch the data from the source
			pipeline.operators[current_idx]->GetData(
				context,
				current_chunk,
				*pipeline.source_state,
				*local_source_state);
		} else {
			// if current_idx > source_idx, we pass the previous' operators output through the Execute of the current operator
			if (pipeline.operators[current_idx]->Execute(
				context,
				*intermediate_chunks[current_intermediate - 1],
				current_chunk,
				*intermediate_states[current_intermediate - 1])) {
				// more data remains in this operator
				// push in-process marker
				in_process_operators.push(current_idx);
			}
		}
		EndOperator(pipeline.operators[current_idx], &current_chunk);
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
			if (current_idx >= pipeline.operators.size()) {
				// if we got output and are at the last operator, we are finished executing for this output chunk
				// return the data and push it into the chunk
				break;
			}
		}
	}
}

void PipelineExecutor::InitializeChunk(DataChunk &chunk) {
	D_ASSERT(!pipeline.operators.empty());
	PhysicalOperator *last_op = pipeline.operators.back();
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
