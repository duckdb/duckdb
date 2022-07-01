#include "duckdb/execution/operator/helper/physical_explain_analyze.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/query_profiler.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class ExplainAnalyzeStateGlobalState : public GlobalSinkState {
public:
	string analyzed_plan;
};

SinkResultType PhysicalExplainAnalyze::Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
                                            DataChunk &input) const {
	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType PhysicalExplainAnalyze::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                  GlobalSinkState &gstate_p) const {
	auto &gstate = (ExplainAnalyzeStateGlobalState &)gstate_p;
	auto &profiler = QueryProfiler::Get(context);
	gstate.analyzed_plan = profiler.ToString();
	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSinkState> PhysicalExplainAnalyze::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<ExplainAnalyzeStateGlobalState>();
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class ExplainAnalyzeState : public GlobalSourceState {
public:
	ExplainAnalyzeState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalExplainAnalyze::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<ExplainAnalyzeState>();
}

void PhysicalExplainAnalyze::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &source_state,
                                     LocalSourceState &lstate) const {
	auto &state = (ExplainAnalyzeState &)source_state;
	auto &gstate = (ExplainAnalyzeStateGlobalState &)*sink_state;
	if (state.finished) {
		return;
	}
	chunk.SetValue(0, 0, Value("analyzed_plan"));
	chunk.SetValue(1, 0, Value(gstate.analyzed_plan));
	chunk.SetCardinality(1);

	state.finished = true;
}

} // namespace duckdb
