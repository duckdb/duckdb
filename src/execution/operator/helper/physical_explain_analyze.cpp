#include "duckdb/execution/operator/helper/physical_explain_analyze.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/query_profiler.hpp"

namespace duckdb {

void AppendExplainLines(const string &text, DataChunk &chunk, ColumnDataCollection *collection) {
	idx_t pos = 0;
	while (pos < text.size()) {
		auto nl = text.find('\n', pos);
		auto line = text.substr(pos, nl == string::npos ? string::npos : nl - pos);
		pos = nl == string::npos ? text.size() : nl + 1;
		if (line.empty()) {
			continue;
		}
		chunk.SetValue(0, chunk.size(), Value(std::move(line)));
		chunk.SetCardinality(chunk.size() + 1);
		if (chunk.size() == STANDARD_VECTOR_SIZE) {
			if (collection) {
				collection->Append(chunk);
				chunk.Reset();
			} else {
				break;
			}
		}
	}
}

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
	AppendExplainLines(gstate.analyzed_plan, chunk);

	return SourceResultType::FINISHED;
}

} // namespace duckdb
