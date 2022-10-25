#include "duckdb/execution/operator/join/physical_join.hpp"

#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {

PhysicalJoin::PhysicalJoin(LogicalOperator &op, PhysicalOperatorType type, JoinType join_type,
                           idx_t estimated_cardinality)
    : PhysicalOperator(type, op.types, estimated_cardinality), join_type(join_type) {
}

bool PhysicalJoin::EmptyResultIfRHSIsEmpty() const {
	// empty RHS with INNER, RIGHT or SEMI join means empty result set
	switch (join_type) {
	case JoinType::INNER:
	case JoinType::RIGHT:
	case JoinType::SEMI:
		return true;
	default:
		return false;
	}
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalJoin::BuildJoinPipelines(Pipeline &current, MetaPipeline &meta_pipeline,
                                      vector<Pipeline *> &final_pipelines, PhysicalOperator &op) {
	op.op_state.reset();
	op.sink_state.reset();

	// 'current' is the probe pipeline: add this operator
	auto &state = meta_pipeline.GetState();
	state.AddPipelineOperator(current, &op);

	// Join can become a source operator if it's RIGHT/OUTER, or if the hash join goes out-of-core
	// this pipeline has to happen AFTER all the probing has happened
	bool add_child_pipeline = false;
	if (op.type != PhysicalOperatorType::CROSS_PRODUCT) {
		auto &join_op = (PhysicalJoin &)op;
		if (IsRightOuterJoin(join_op.join_type)) {
			if (meta_pipeline.HasRecursiveCTE()) {
				throw NotImplementedException("FULL and RIGHT outer joins are not supported in recursive CTEs yet");
			}
			add_child_pipeline = true;
		}

		if (join_op.type == PhysicalOperatorType::HASH_JOIN) {
			auto &hash_join_op = (PhysicalHashJoin &)join_op;
			hash_join_op.can_go_external = !meta_pipeline.HasRecursiveCTE();
			if (hash_join_op.can_go_external) {
				add_child_pipeline = true;
			}
		}
	}

	if (add_child_pipeline) {
		// create child pipeline
		auto child_pipeline = meta_pipeline.CreateChildPipeline(current);
		// create a new vector to set up dependencies
		vector<Pipeline *> child_pipeline_dependencies;
		// continue building the LHS pipeline (probe child)
		op.children[0]->BuildPipelines(current, meta_pipeline, child_pipeline_dependencies);
		// child pipeline depends on the downstream child pipelines to have finished (if any)
		for (auto dependee : child_pipeline_dependencies) {
			meta_pipeline.AddInterPipelineDependency(child_pipeline, dependee);
		}
		// the child pipeline needs to finish before the MetaPipeline is finished
		final_pipelines.push_back(child_pipeline);
	} else {
		// continue building the LHS pipeline (probe child)
		op.children[0]->BuildPipelines(current, meta_pipeline, final_pipelines);
	}

	// on the RHS (build side), we construct a new child pipeline with this pipeline as its source
	auto child_meta_pipeline = meta_pipeline.CreateChildMetaPipeline(current, &op);
	child_meta_pipeline->Build(op.children[1].get());
}

void PhysicalJoin::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline, vector<Pipeline *> &final_pipelines) {
	PhysicalJoin::BuildJoinPipelines(current, meta_pipeline, final_pipelines, *this);
}

vector<const PhysicalOperator *> PhysicalJoin::GetSources() const {
	auto result = children[0]->GetSources();
	if (IsSource()) {
		result.push_back(this);
	}
	return result;
}

} // namespace duckdb
