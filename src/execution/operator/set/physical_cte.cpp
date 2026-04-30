#include "duckdb/execution/operator/set/physical_cte.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {

PhysicalCTE::PhysicalCTE(PhysicalPlan &physical_plan, string ctename, TableIndex table_index, vector<LogicalType> types,
                         PhysicalOperator &top, PhysicalOperator &bottom, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::CTE, std::move(types), estimated_cardinality),
      table_index(table_index), ctename(std::move(ctename)) {
	children.push_back(top);
	children.push_back(bottom);
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
	child_meta_pipeline.Build(children[0]);

	for (auto &cte_scan : cte_scans) {
		state.cte_dependencies.insert(make_pair(cte_scan, reference<Pipeline>(*child_meta_pipeline.GetBasePipeline())));
	}

	// If the CTE body is a DML statement (INSERT/UPDATE/DELETE/MERGE INTO), all MetaPipelines
	// created while building children[1] (the query side) must run after the DML completes.
	// We follow the same pattern as PhysicalJoin::BuildJoinPipelines: capture the DML pipelines
	// and the current last child before building children[1], then call AddRecursiveDependencies
	// with force=true so that ordering is always enforced (not just when pipelines exceed the
	// thread count, as is the case for join build dependencies).
	vector<shared_ptr<Pipeline>> dml_pipelines;
	optional_ptr<MetaPipeline> last_child_ptr;
	if (cte_body_is_dml) {
		child_meta_pipeline.GetPipelines(dml_pipelines, false);
		last_child_ptr = meta_pipeline.GetLastChild();
	}

	children[1].get().BuildPipelines(current, meta_pipeline);

	if (last_child_ptr) {
		meta_pipeline.AddRecursiveDependencies(dml_pipelines, *last_child_ptr, true);
	}
}

vector<const_reference<PhysicalOperator>> PhysicalCTE::GetSources() const {
	return children[1].get().GetSources();
}

InsertionOrderPreservingMap<string> PhysicalCTE::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["CTE Name"] = ctename;
	result["Table Index"] = StringUtil::Format("%llu", table_index.index);
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

} // namespace duckdb
