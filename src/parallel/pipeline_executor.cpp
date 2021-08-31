#include "duckdb/parallel/pipeline_executor.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

PipelineExecutor::PipelineExecutor(ClientContext &context_p, Pipeline &pipeline_p) :
	pipeline(pipeline_p), thread(context_p), context(context_p, thread) {
	D_ASSERT(pipeline.source);
	D_ASSERT(pipeline.source_state);
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
			Execute(final_chunk);
			if (final_chunk.size() == 0) {
				pipeline.sink->Combine(context, *pipeline.sink_state, *local_sink_state);
				break;
			}
			StartOperator(pipeline.sink);
			pipeline.sink->Sink(context, *pipeline.sink_state, *local_sink_state, final_chunk);
			EndOperator(pipeline.sink, nullptr);
		}
	} catch (std::exception &ex) {
		executor.PushError(ex.what());
	} catch (...) { // LCOV_EXCL_START
		executor.PushError("Unknown exception in pipeline!");
	} // LCOV_EXCL_STOP
	executor.Flush(thread);
}

void PipelineExecutor::Execute(DataChunk &result) {
	if (context.client.interrupted) {
		return;
	}

	idx_t current_idx = 0;
	while(true) {
		if (!in_process_operators.empty()) {
			current_idx = in_process_operators.top();
			in_process_operators.pop();
		}
		auto &current_chunk = current_idx >= intermediate_chunks.size() ? result : *intermediate_chunks[current_idx];
		current_chunk.Reset();
		if (current_idx == 0) {
			pipeline.source->GetData(
				context,
				current_chunk,
				*pipeline.source_state,
				*local_source_state);
		} else {
			if (pipeline.operators[current_idx - 1]->Execute(
				context,
				*intermediate_chunks[current_idx - 1],
				current_chunk,
				*intermediate_states[current_idx - 1])) {
				// more data remains in this operator
				// push in-process marker
				in_process_operators.push(current_idx);
			}
		}
		if (current_chunk.size() == 0) {
			if (current_idx == 0) {
				break;
			} else {
				current_idx = 0;
				continue;
			}
		} else {
			current_idx++;
			if (current_idx > pipeline.operators.size()) {
				break;
			}
		}
	}
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

	// reset the chunk back to its initial state
	// chunk.Reset();
}

void PipelineExecutor::EndOperator(PhysicalOperator *op, DataChunk *chunk) {
	context.thread.profiler.EndOperator(chunk);

	if (chunk) {
		chunk->Verify();
	}
}

}
