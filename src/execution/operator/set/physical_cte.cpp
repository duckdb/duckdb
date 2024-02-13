#include "duckdb/execution/operator/set/physical_cte.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/parallel/event.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {

PhysicalCTE::PhysicalCTE(string ctename, idx_t table_index, vector<LogicalType> types, unique_ptr<PhysicalOperator> top,
                         unique_ptr<PhysicalOperator> bottom, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::CTE, std::move(types), estimated_cardinality), table_index(table_index),
      ctename(std::move(ctename)) {
	children.push_back(std::move(top));
	children.push_back(std::move(bottom));
}

PhysicalCTE::~PhysicalCTE() {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class CTEGlobalState : public GlobalSinkState {
public:
	explicit CTEGlobalState(ClientContext &context, const PhysicalCTE &op) : working_table_ref(op.working_table.get()) {
	}
	optional_ptr<ColumnDataCollection> working_table_ref;

	mutex lhs_lock;

	void MergeIT(ColumnDataCollection &input) {
		lock_guard<mutex> guard(lhs_lock);
		working_table_ref->Combine(input);
	}
};

class CTELocalState : public LocalSinkState {
public:
	explicit CTELocalState(ClientContext &context, const PhysicalCTE &op)
	    : lhs_data(context, op.working_table->Types()) {
		lhs_data.InitializeAppend(append_state);
	}

	unique_ptr<LocalSinkState> distinct_state;
	ColumnDataCollection lhs_data;
	ColumnDataAppendState append_state;

	void Append(DataChunk &input) {
		lhs_data.Append(input);
	}
};

unique_ptr<GlobalSinkState> PhysicalCTE::GetGlobalSinkState(ClientContext &context) const {
	working_table->Reset();
	return make_uniq<CTEGlobalState>(context, *this);
}

unique_ptr<LocalSinkState> PhysicalCTE::GetLocalSinkState(ExecutionContext &context) const {
	auto state = make_uniq<CTELocalState>(context.client, *this);
	return std::move(state);
}

SinkResultType PhysicalCTE::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &lstate = input.local_state.Cast<CTELocalState>();
	lstate.lhs_data.Append(lstate.append_state, chunk);

	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalCTE::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &lstate = input.local_state.Cast<CTELocalState>();
	auto &gstate = input.global_state.Cast<CTEGlobalState>();
	gstate.MergeIT(lstate.lhs_data);

	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalCTE::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	D_ASSERT(children.size() == 2);
	op_state.reset();
	sink_state.reset();

	auto &state = meta_pipeline.GetState();

	auto &child_meta_pipeline = meta_pipeline.CreateChildMetaPipeline(current, *this);
	child_meta_pipeline.Build(*children[0]);

	for (auto &cte_scan : cte_scans) {
		state.cte_dependencies.insert(make_pair(cte_scan, reference<Pipeline>(*child_meta_pipeline.GetBasePipeline())));
	}

	children[1]->BuildPipelines(current, meta_pipeline);
}

vector<const_reference<PhysicalOperator>> PhysicalCTE::GetSources() const {
	return children[1]->GetSources();
}

string PhysicalCTE::ParamsToString() const {
	string result = "";
	result += "\n[INFOSEPARATOR]\n";
	result += ctename;
	result += "\n[INFOSEPARATOR]\n";
	result += StringUtil::Format("idx: %llu", table_index);
	return result;
}

} // namespace duckdb
