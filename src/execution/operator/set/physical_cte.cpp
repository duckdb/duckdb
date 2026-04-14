#include "duckdb/execution/operator/set/physical_cte.hpp"

#include "duckdb/common/reference_map.hpp"
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
static bool ContainsDML(const PhysicalOperator &op) {
	switch (op.type) {
	case PhysicalOperatorType::INSERT:
	case PhysicalOperatorType::BATCH_INSERT:
	case PhysicalOperatorType::DELETE_OPERATOR:
	case PhysicalOperatorType::UPDATE:
		return true;
	default:
		break;
	}
	for (auto &child : op.children) {
		if (ContainsDML(child.get())) {
			return true;
		}
	}
	return false;
}

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

	// If the CTE body contains DML (INSERT/UPDATE/DELETE), the query side must run after
	// the DML completes so that it sees the modified table state.  The dependency on
	// `current` is already established by CreateChildMetaPipeline above, but child
	// MetaPipelines spawned while building children[1] (e.g. the scan pipeline under an
	// aggregate) are in separate MetaPipelines and would otherwise race with the DML.
	// Capture the DML base pipeline before building children[1] so we can add explicit
	// dependencies to any new child MetaPipelines that are created during that build.
	const bool cte_has_dml = ContainsDML(children[0].get());
	vector<shared_ptr<MetaPipeline>> child_meta_pipelines_before;
	if (cte_has_dml) {
		meta_pipeline.GetMetaPipelines(child_meta_pipelines_before, false, true);
	}

	children[1].get().BuildPipelines(current, meta_pipeline);

	if (cte_has_dml) {
		// Collect the child MetaPipelines that were created while building children[1].
		vector<shared_ptr<MetaPipeline>> child_meta_pipelines_after;
		meta_pipeline.GetMetaPipelines(child_meta_pipelines_after, false, true);

		reference_set_t<MetaPipeline> before_set;
		for (auto &mp : child_meta_pipelines_before) {
			before_set.insert(*mp);
		}

		auto &dml_base_pipeline = child_meta_pipeline.GetBasePipeline();
		for (auto &mp : child_meta_pipelines_after) {
			if (before_set.find(*mp) != before_set.end()) {
				continue; // existed before, skip
			}
			// All new child MetaPipelines must wait for the DML pipeline to complete.
			mp->GetBasePipeline()->AddDependency(dml_base_pipeline);
		}
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
