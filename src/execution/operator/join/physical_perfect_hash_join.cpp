#include "duckdb/execution/operator/join/physical_perfect_hash_join.hpp"

namespace duckdb {

PhysicalPerfectHashJoin::PhysicalPerfectHashJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                                 unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond,
                                                 JoinType join_type, const vector<idx_t> &left_projection_map,
                                                 const vector<idx_t> &right_projection_map_p,
                                                 vector<LogicalType> delim_types, idx_t estimated_cardinality)
    : PhysicalComparisonJoin(op, PhysicalOperatorType::HASH_JOIN, move(cond), join_type, estimated_cardinality),
      right_projection_map(right_projection_map_p), delim_types(move(delim_types)) {
	children.push_back(move(left));
	children.push_back(move(right));

	D_ASSERT(left_projection_map.size() == 0);
	for (auto &condition : conditions) {
		condition_types.push_back(condition.left->return_type);
	}

	// for ANTI, SEMI and MARK join, we only need to store the keys, so for these the build types are empty
	if (join_type != JoinType::ANTI && join_type != JoinType::SEMI && join_type != JoinType::MARK) {
		build_types = LogicalOperator::MapTypes(children[1]->GetTypes(), right_projection_map);
	}
}

PhysicalPerfectHashJoin::PhysicalPerfectHashJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                                 unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond,
                                                 JoinType join_type, idx_t estimated_cardinality)
    : PhysicalPerfectHashJoin(op, move(left), move(right), move(cond), join_type, {}, {}, {}, estimated_cardinality) {
}

unique_ptr<GlobalOperatorState> PhysicalPerfectHashJoin::GetGlobalState(ClientContext &context) {
}

unique_ptr<LocalSinkState> PhysicalPerfectHashJoin::GetLocalSinkState(ExecutionContext &context) {
	auto state = make_unique<HashJoinLocalState>();
	if (!right_projection_map.empty()) {
		state->build_chunk.Initialize(build_types);
	}
	for (auto &cond : conditions) {
		state->build_executor.AddExpression(*cond.right);
	}
	state->join_keys.Initialize(condition_types);
	return move(state);
}
void PhysicalPerfectHashJoin::Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate,
                                   DataChunk &input) {
}
void PhysicalPerfectHashJoin::Finalize(Pipeline &pipeline, ClientContext &context,
                                       unique_ptr<GlobalOperatorState> gstate) {
}

void PhysicalPerfectHashJoin::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
                                               PhysicalOperatorState *state) {
}
unique_ptr<PhysicalOperatorState> PhysicalPerfectHashJoin::GetOperatorState() {
}
} // namespace duckdb