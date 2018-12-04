#include "execution/operator/join/physical_hash_join.hpp"

#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"

using namespace duckdb;
using namespace std;

PhysicalHashJoin::PhysicalHashJoin(LogicalOperator &op, std::unique_ptr<PhysicalOperator> left,
                                   std::unique_ptr<PhysicalOperator> right, std::vector<JoinCondition> cond,
                                   JoinType join_type)
    : PhysicalJoin(op, PhysicalOperatorType::HASH_JOIN, move(cond), join_type) {
	hash_table = make_unique<JoinHashTable>(conditions, right->GetTypes(), join_type);

	children.push_back(move(left));
	children.push_back(move(right));
}

void PhysicalHashJoin::_GetChunk(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalHashJoinOperatorState *>(state_);
	if (!state->initialized) {
		// build the HT
		auto right_state = children[1]->GetOperatorState(state->parent);
		auto types = children[1]->GetTypes();

		DataChunk right_chunk;
		right_chunk.Initialize(types);

		state->join_keys.Initialize(hash_table->condition_types);
		while (true) {
			// get the child chunk
			children[1]->GetChunk(context, right_chunk, right_state.get());
			if (right_chunk.size() == 0) {
				break;
			}
			// resolve the join keys for the right chunk
			state->join_keys.Reset();
			ExpressionExecutor executor(right_chunk, context);
			for (size_t i = 0; i < conditions.size(); i++) {
				executor.ExecuteExpression(conditions[i].right.get(), state->join_keys.data[i]);
			}
			// build the HT
			hash_table->Build(state->join_keys, right_chunk);
		}

		if (hash_table->size() == 0 && hash_table->join_type != JoinType::ANTI) {
			// empty hash table means empty result set
			// except for ANTI-join, in which case it means NOP join
			// FIXME: NOP join should not involve HT at all
			return;
		}

		state->initialized = true;
	}
	if (state->scan_structure) {
		// still have elements remaining from the previous probe (i.e. we got
		// >1024 elements in the previous probe)
		state->scan_structure->Next(state->join_keys, state->child_chunk, chunk);
		if (chunk.size() > 0) {
			return;
		}
		state->scan_structure = nullptr;
	}

	// probe the HT
	do {
		// fetch the chunk from the left side
		children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
		if (state->child_chunk.size() == 0) {
			return;
		}
		// remove any selection vectors
		state->child_chunk.Flatten();
		// resolve the join keys for the left chunk
		state->join_keys.Reset();
		ExpressionExecutor executor(state->child_chunk, context);
		for (size_t i = 0; i < conditions.size(); i++) {
			executor.ExecuteExpression(conditions[i].left.get(), state->join_keys.data[i]);
		}
		// perform the actual probe
		state->scan_structure = hash_table->Probe(state->join_keys);
		state->scan_structure->Next(state->join_keys, state->child_chunk, chunk);
	} while (chunk.size() == 0);
}

std::unique_ptr<PhysicalOperatorState> PhysicalHashJoin::GetOperatorState(ExpressionExecutor *parent_executor) {
	return make_unique<PhysicalHashJoinOperatorState>(children[0].get(), children[1].get(), parent_executor);
}
