#include "duckdb/execution/operator/join/physical_join.hpp"

#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {

PhysicalJoin::PhysicalJoin(PhysicalPlan &physical_plan, LogicalOperator &op, PhysicalOperatorType type,
                           JoinType join_type, idx_t estimated_cardinality)
    : CachingPhysicalOperator(physical_plan, type, op.types, estimated_cardinality), join_type(join_type) {
}

bool PhysicalJoin::EmptyResultIfRHSIsEmpty() const {
	// empty RHS with INNER, RIGHT or SEMI join means empty result set
	switch (join_type) {
	case JoinType::INNER:
	case JoinType::RIGHT:
	case JoinType::SEMI:
	case JoinType::RIGHT_SEMI:
	case JoinType::RIGHT_ANTI:
		return true;
	default:
		return false;
	}
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
static bool ScansJoinFilterTarget(Pipeline &pipeline, const JoinFilterPushdownInfo &filter_pushdown) {
	auto source = pipeline.GetSource();
	if (!source || source->type != PhysicalOperatorType::TABLE_SCAN) {
		return false;
	}
	auto &dynamic_filters = source->Cast<PhysicalTableScan>().dynamic_filters;
	if (!dynamic_filters) {
		return false;
	}
	for (auto &info : filter_pushdown.probe_info) {
		if (info.dynamic_filters && RefersToSameObject(*info.dynamic_filters, *dynamic_filters)) {
			return true;
		}
	}
	return false;
}

//! Join filters are pushed into the probe-side scans when the build side is finalized, and a scan only picks them up
//! when its global source state is initialized. A probe-side pipeline below a blocking operator (e.g., an aggregate
//! that groups on the join key) does not depend on the build side, so its scan would start without the filters.
//! Make the probe-side pipelines that scan a target of the join filters wait until the build side has finished.
static void AddJoinFilterDependencies(Pipeline &current, MetaPipeline &meta_pipeline, PhysicalOperator &op,
                                      idx_t build_child_idx) {
	switch (op.type) {
	case PhysicalOperatorType::HASH_JOIN:
	case PhysicalOperatorType::NESTED_LOOP_JOIN:
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
		break;
	default:
		return;
	}
	auto &filter_pushdown = op.Cast<PhysicalComparisonJoin>().filter_pushdown;
	if (!filter_pushdown || filter_pushdown->probe_info.empty() || meta_pipeline.HasRecursiveCTE()) {
		return;
	}
	auto &children = meta_pipeline.GetChildren();
	auto &build_pipeline = children[build_child_idx]->GetBasePipeline();
	// the child MetaPipelines that were created after the build side belong to the probe side
	for (idx_t child_idx = build_child_idx + 1; child_idx < children.size(); child_idx++) {
		auto &child = *children[child_idx];
		if (child.Type() == MetaPipelineType::JOIN_BUILD && RefersToSameObject(*child.GetParent(), current)) {
			// sibling join builds only finalize once all of them have executed: waiting for one would be cyclic
			continue;
		}
		vector<shared_ptr<Pipeline>> pipelines;
		child.GetPipelines(pipelines, true);
		for (auto &pipeline : pipelines) {
			if (ScansJoinFilterTarget(*pipeline, *filter_pushdown)) {
				pipeline->AddDependency(build_pipeline);
			}
		}
	}
}

void PhysicalJoin::BuildJoinPipelines(Pipeline &current, MetaPipeline &meta_pipeline, PhysicalOperator &op,
                                      bool build_rhs) {
	op.op_state.reset();
	op.sink_state.reset();

	// 'current' is the probe pipeline: add this operator
	auto &state = meta_pipeline.GetState();
	state.AddPipelineOperator(current, op);

	// save the last added pipeline to set up dependencies later (in case we need to add a child pipeline)
	vector<shared_ptr<Pipeline>> pipelines_so_far;
	meta_pipeline.GetPipelines(pipelines_so_far, false);
	auto &last_pipeline = *pipelines_so_far.back();

	vector<shared_ptr<Pipeline>> dependencies;
	optional_ptr<MetaPipeline> last_child_ptr;
	optional_idx build_child_idx;
	if (build_rhs) {
		// on the RHS (build side), we construct a child MetaPipeline with this operator as its sink
		build_child_idx = meta_pipeline.GetChildren().size();
		auto &child_meta_pipeline = meta_pipeline.CreateChildMetaPipeline(current, op, MetaPipelineType::JOIN_BUILD);
		child_meta_pipeline.Build(op.children[1]);
		if (op.children[1].get().CanSaturateThreads(current.GetClientContext())) {
			// if the build side can saturate all available threads,
			// we don't just make the LHS pipeline depend on the RHS, but recursively all LHS children too.
			// this prevents breadth-first plan evaluation
			child_meta_pipeline.GetPipelines(dependencies, false);
			last_child_ptr = meta_pipeline.GetLastChild();
		}
	}

	// continue building the current pipeline on the LHS (probe side)
	op.children[0].get().BuildPipelines(current, meta_pipeline);

	if (last_child_ptr) {
		// the pointer was set, set up the dependencies
		meta_pipeline.AddRecursiveDependencies(dependencies, *last_child_ptr);
	}
	if (build_child_idx.IsValid()) {
		AddJoinFilterDependencies(current, meta_pipeline, op, build_child_idx.GetIndex());
	}

	switch (op.type) {
	case PhysicalOperatorType::POSITIONAL_JOIN:
		// Positional joins are always outer
		meta_pipeline.CreateChildPipeline(current, op, last_pipeline);
		return;
	case PhysicalOperatorType::CROSS_PRODUCT:
		return;
	default:
		break;
	}

	// Join can become a source operator if it's RIGHT/OUTER, or if the hash join goes out-of-core
	if (op.Cast<PhysicalJoin>().IsSource()) {
		meta_pipeline.CreateChildPipeline(current, op, last_pipeline);
	}
}

void PhysicalJoin::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	PhysicalJoin::BuildJoinPipelines(current, meta_pipeline, *this);
}

vector<const_reference<PhysicalOperator>> PhysicalJoin::GetSources() const {
	auto result = children[0].get().GetSources();
	if (IsSource()) {
		result.push_back(*this);
	}
	return result;
}

vector<idx_t> PhysicalJoin::FillProjectionMap(const PhysicalOperator &child,
                                              const vector<ProjectionIndex> &projection_map) {
	const auto child_count = child.GetTypes().size();

	vector<idx_t> result;
	if (projection_map.empty()) {
		// no projections - fill with all children
		result.reserve(child_count);
		for (idx_t i = 0; i < child_count; ++i) {
			result.emplace_back(i);
		}
	} else {
		for (auto &entry : projection_map) {
			if (entry >= child_count) {
				throw InternalException("Projection map entry %d out of range", entry);
			}
			result.emplace_back(entry);
		}
	}
	return result;
}

} // namespace duckdb
