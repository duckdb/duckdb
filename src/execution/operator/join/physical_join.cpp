#include "duckdb/execution/operator/join/physical_join.hpp"
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
void PhysicalJoin::BuildJoinPipelines(Executor &executor, Pipeline &current, PipelineBuildState &state,
                                      PhysicalOperator &op) {
	op.op_state.reset();
	op.sink_state.reset();

	// on the LHS (probe child), the operator becomes a regular operator
	state.AddPipelineOperator(current, &op);
	if (op.IsSource()) {
		// FULL or RIGHT outer join
		// schedule a scan of the node as a child pipeline
		// this scan has to be performed AFTER all the probing has happened
		if (state.recursive_cte) {
			throw NotImplementedException("FULL and RIGHT outer joins are not supported in recursive CTEs yet");
		}
		state.AddChildPipeline(executor, current);
	}
	// continue building the pipeline on this child
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
