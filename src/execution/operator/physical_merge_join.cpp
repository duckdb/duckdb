
#include "execution/operator/physical_merge_join.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"

using namespace duckdb;
using namespace std;

// struct MergeInfo {
// 	Vector &vector;
// 	sel_t *order;
// 	size_t &position;

// 	MergeInfo(Vector &vector, sel_t *order, size_t &position) :
// 		vector(vector), order(order), position(position) { }
// };

// static size_t MergeJoin(MergeInfo &left, MergeInfo &right, ExpressionType
// comparison_type, sel_t result[]) { 	size_t result_count = 0;

// 	if (comparison_type == ExpressionType::COMPARE_LESSTHAN) {

// 	} else if (comparison_type == ExpressionType::COMPARE_LESSTHANOREQUALTO) {

// 	} else if (comparison_type == ExpressionType::COMPARE_GREATERTHAN ||
// 			   comparison_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO)
// {
// 		// simply flip the comparison type
// 		return MergeJoin(right, left,
// LogicalJoin::FlipComparisionExpression(comparison_type), result); 	} else {
// 		throw Exception("Unimplemented comparison type for merge join!");
// 	}
// 	return result_count;
// }

PhysicalMergeJoin::PhysicalMergeJoin(std::unique_ptr<PhysicalOperator> left,
                                     std::unique_ptr<PhysicalOperator> right,
                                     std::vector<JoinCondition> cond,
                                     JoinType join_type)
    : PhysicalJoin(PhysicalOperatorType::MERGE_JOIN, move(cond), join_type) {
	// for now we only support one condition!
	assert(conditions.size() == 1);
	for (auto &cond : conditions) {
		// COMPARE NOT EQUAL not supported yet with merge join
		assert(cond.comparison != ExpressionType::COMPARE_NOTEQUAL);
		assert(cond.left->return_type == cond.right->return_type);
		join_key_types.push_back(cond.left->return_type);
	}
}

void PhysicalMergeJoin::_GetChunk(ClientContext &context, DataChunk &chunk,
                                  PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalMergeJoinOperatorState *>(state_);
	chunk.Reset();

	if (!state->initialized) {
		// create the sorted pieces
		auto right_state = children[1]->GetOperatorState(state->parent);
		auto types = children[1]->GetTypes();

		DataChunk right_chunk;
		right_chunk.Initialize(types);
		state->join_keys.Initialize(join_key_types);
		// first fetch the entire right side
		while (true) {
			children[1]->GetChunk(context, right_chunk, right_state.get());
			if (right_chunk.size() == 0) {
				break;
			}
			state->right_chunks.Append(right_chunk);
		}
		if (state->right_chunks.count == 0) {
			return;
		}
		// now order all the chunks
		state->right_orders.resize(state->right_chunks.chunks.size());
		for (size_t i = 0; i < state->right_chunks.chunks.size(); i++) {
			auto &chunk_to_order = *state->right_chunks.chunks[i];
			// create a new selection vector
			auto selection_vectors =
			    unique_ptr<sel_t[]>(new sel_t[chunk_to_order.size()]);
			// resolve the join keys for the right chunk
			state->join_keys.Reset();
			ExpressionExecutor executor(chunk_to_order, context);
			for (size_t i = 0; i < conditions.size(); i++) {
				// resolve the join key
				executor.ExecuteExpression(conditions[i].right.get(),
				                           state->join_keys.data[i]);
				// sort by the join key
				VectorOperations::Sort(state->join_keys.data[i],
				                       selection_vectors.get());
			}
			state->right_orders[i] = move(selection_vectors);
		}
		state->right_chunk_index = state->right_orders.size();
		state->initialized = true;
	}

	do {
		// check if we have to fetch a child from the left side
		if (state->right_chunk_index == state->right_orders.size()) {
			// fetch the chunk from the left side
			children[0]->GetChunk(context, state->child_chunk,
			                      state->child_state.get());
			if (state->child_chunk.size() == 0) {
				return;
			}

			// resolve the join keys for the left chunk
			state->join_keys.Reset();
			ExpressionExecutor executor(state->child_chunk, context);
			for (size_t i = 0; i < conditions.size(); i++) {
				executor.ExecuteExpression(conditions[i].left.get(),
				                           state->join_keys.data[i]);
				// sort by join key
				VectorOperations::Sort(state->join_keys.data[i],
				                       state->left_orders.get());
			}
			state->right_chunk_index = 0;
			state->left_position = 0;
			state->right_position = 0;
		}

		throw NotImplementedException("FIXME: actual merge join");
		// auto &right_chunk =
		// state->right_chunks.chunks[state->right_chunk_index]; auto
		// &right_orders = state->right_orders[state->right_chunk_index];

		// MergeInfo left(state->join_keys.data[0], state->left_orders.get(),
		// state->left_position); MergeInfo right(/* FIXME */,
		// right_orders.get(), state->right_position); sel_t
		// result[STANDARD_VECTOR_SIZE];
		// perform the merge join
		// size_t result_count = MergeJoin(left, right, conditions[0].type,
		// result);
		// if (result_count == 0) {
		// 	// exhausted this chunk on the right side
		// 	// move to the next
		// 	state->right_chunk_index++;
		// 	state->left_position = 0;
		// 	state->right_position = 0;
		// } else {
		// 	// FIXME: create the result chunk from the selection vector
		// }
	} while (chunk.size() == 0);
}

std::unique_ptr<PhysicalOperatorState>
PhysicalMergeJoin::GetOperatorState(ExpressionExecutor *parent_executor) {
	return make_unique<PhysicalMergeJoinOperatorState>(
	    children[0].get(), children[1].get(), parent_executor);
}
