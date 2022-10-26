#include "duckdb/execution/operator/join/physical_join.hpp"

#include "duckdb/execution/operator/join/physical_hash_join.hpp"
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
		return true;
	default:
		return false;
	}
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalJoin::BuildJoinPipelines(Executor &executor, Pipeline &current, PipelineBuildState &state,
                                      PhysicalOperator &op) {
	op.op_state.reset();
	op.sink_state.reset();

	// 'current' is the probe pipeline: add this operator
	state.AddPipelineOperator(current, &op);

	// Join can become a source operator if it's RIGHT/OUTER, or if the hash join goes out-of-core
	// this pipeline has to happen AFTER all the probing has happened
	bool add_child_pipeline = false;
	if (op.type != PhysicalOperatorType::CROSS_PRODUCT) {
		auto &join_op = (PhysicalJoin &)op;
		if (IsRightOuterJoin(join_op.join_type)) {
			if (state.recursive_cte) {
				throw NotImplementedException("FULL and RIGHT outer joins are not supported in recursive CTEs yet");
			}
			add_child_pipeline = true;
		}

		if (join_op.type == PhysicalOperatorType::HASH_JOIN) {
			auto &hash_join_op = (PhysicalHashJoin &)join_op;
			hash_join_op.can_go_external = !state.recursive_cte && !IsRightOuterJoin(join_op.join_type) &&
			                               join_op.join_type != JoinType::ANTI && join_op.join_type != JoinType::MARK;
			if (hash_join_op.can_go_external) {
				add_child_pipeline = true;
			}
		}

		if (add_child_pipeline) {
			state.AddChildPipeline(executor, current);
		}
	}

	// continue building the LHS pipeline (probe child)
	op.children[0]->BuildPipelines(executor, current, state);

	// on the RHS (build side), we construct a new child pipeline with this pipeline as its source
	op.BuildChildPipeline(executor, current, state, op.children[1].get());
}

void PhysicalJoin::BuildPipelines(Executor &executor, Pipeline &current, PipelineBuildState &state) {
	PhysicalJoin::BuildJoinPipelines(executor, current, state, *this);
}

vector<const PhysicalOperator *> PhysicalJoin::GetSources() const {
	auto result = children[0]->GetSources();
	if (IsSource()) {
		result.push_back(this);
	}
	return result;
}

} // namespace duckdb
