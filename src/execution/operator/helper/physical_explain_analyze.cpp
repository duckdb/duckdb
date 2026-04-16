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

SinkResultType PhysicalExplainAnalyze::Sink(ExecutionContext &context, DataChunk &chunk,
                                            OperatorSinkInput &input) const {
	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType PhysicalExplainAnalyze::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                  OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<ExplainAnalyzeStateGlobalState>();
	auto &profiler = QueryProfiler::Get(context);
	profiler.FinalizeMetrics();
	gstate.analyzed_plan = profiler.ToString(format);
	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSinkState> PhysicalExplainAnalyze::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<ExplainAnalyzeStateGlobalState>();
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalExplainAnalyze::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                         OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<ExplainAnalyzeStateGlobalState>();

	// Split plan into individual lines (PostgreSQL-compatible single-column format).
	auto &plan = gstate.analyzed_plan;
	idx_t row = 0;
	idx_t pos = 0;
	while (pos < plan.size() && row < STANDARD_VECTOR_SIZE) {
		auto nl = plan.find('\n', pos);
		auto line = plan.substr(pos, nl == string::npos ? string::npos : nl - pos);
		pos = nl == string::npos ? plan.size() : nl + 1;
		if (line.empty()) {
			continue;
		}
		chunk.SetValue(0, row, Value(std::move(line)));
		row++;
	}
	chunk.SetCardinality(row);

	return SourceResultType::FINISHED;
}

} // namespace duckdb
