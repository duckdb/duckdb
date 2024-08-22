#include "duckdb/execution/operator/join/physical_join.hpp"

#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {

PhysicalJoin::PhysicalJoin(LogicalOperator &op, PhysicalOperatorType type, JoinType join_type,
                           idx_t estimated_cardinality)
    : CachingPhysicalOperator(type, op.types, estimated_cardinality), join_type(join_type) {
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
	if (build_rhs) {
		// on the RHS (build side), we construct a child MetaPipeline with this operator as its sink
		auto &child_meta_pipeline = meta_pipeline.CreateChildMetaPipeline(current, op, MetaPipelineType::JOIN_BUILD);
		child_meta_pipeline.Build(*op.children[1]);
		if (op.children[1]->CanSaturateThreads(current.GetClientContext())) {
			// if the build side can saturate all available threads,
			// we don't just make the LHS pipeline depend on the RHS, but recursively all LHS children too.
			// this prevents breadth-first plan evaluation
			child_meta_pipeline.GetPipelines(dependencies, false);
			last_child_ptr = meta_pipeline.GetLastChild();
		}
	}

	// continue building the current pipeline on the LHS (probe side)
	op.children[0]->BuildPipelines(current, meta_pipeline);

	if (last_child_ptr) {
		// the pointer was set, set up the dependencies
		meta_pipeline.AddRecursiveDependencies(dependencies, *last_child_ptr);
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
	auto result = children[0]->GetSources();
	if (IsSource()) {
		result.push_back(*this);
	}
	return result;
}

} // namespace duckdb
