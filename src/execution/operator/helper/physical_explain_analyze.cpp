#include "duckdb/execution/operator/helper/physical_explain_analyze.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/query_profiler.hpp"

namespace duckdb {

class ExplainAnalyzeState : public GlobalSourceState {
public:
	ExplainAnalyzeState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalExplainAnalyze::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<ExplainAnalyzeState>();
}

void PhysicalExplainAnalyze::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate, LocalSourceState &lstate) const {
	auto &state = (ExplainAnalyzeState &) gstate;
	if (state.finished) {
		return;
	}
	string analyzed_plan = context.client.profiler->ToString();
	chunk.SetValue(0, 0, Value("analyzed_plan"));
	chunk.SetValue(1, 0, Value(move(analyzed_plan)));
	chunk.SetCardinality(1);

	state.finished = true;
}


SinkResultType PhysicalExplainAnalyze::Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
					DataChunk &input) const {
	return SinkResultType::NEED_MORE_INPUT;
}

}
