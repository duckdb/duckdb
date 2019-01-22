#include "execution/operator/join/physical_piecewise_merge_join.hpp"

#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"
#include "execution/merge_join.hpp"

using namespace duckdb;
using namespace std;

PhysicalPiecewiseMergeJoin::PhysicalPiecewiseMergeJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                                       unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond,
                                                       JoinType join_type)
    : PhysicalJoin(op, PhysicalOperatorType::PIECEWISE_MERGE_JOIN, move(cond), join_type) {
	// for now we only support one condition!
	assert(conditions.size() == 1);
	for (auto &cond : conditions) {
		// COMPARE NOT EQUAL not supported yet with merge join
		assert(cond.comparison != ExpressionType::COMPARE_NOTEQUAL);
		assert(cond.left->return_type == cond.right->return_type);
		join_key_types.push_back(cond.left->return_type);
	}
	children.push_back(move(left));
	children.push_back(move(right));
}

static void OrderVector(Vector &vector, PhysicalPiecewiseMergeJoin::MergeOrder &order) {
	// first remove any NULL values; they can never match anyway
	sel_t not_null_order[STANDARD_VECTOR_SIZE];
	sel_t *result_vector;
	order.count = Vector::NotNullSelVector(vector, not_null_order, result_vector);
	// sort by the join key
	VectorOperations::Sort(vector, result_vector, order.count, order.order);
}

void PhysicalPiecewiseMergeJoin::_GetChunk(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalPiecewiseMergeJoinOperatorState *>(state_);
	assert(conditions.size() == 1);
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
			// resolve the join keys for the right chunk
			state->join_keys.Reset();
			ExpressionExecutor executor(chunk_to_order, context);
			for (size_t k = 0; k < conditions.size(); k++) {
				// resolve the join key
				executor.ExecuteExpression(conditions[k].right.get(), state->join_keys.data[k]);
				OrderVector(state->join_keys.data[k], state->right_orders[i]);
			}
			state->right_conditions.Append(state->join_keys);
		}
		state->right_chunk_index = state->right_orders.size();
		state->initialized = true;
	}

	do {
		// check if we have to fetch a child from the left side
		if (state->right_chunk_index == state->right_orders.size()) {
			// fetch the chunk from the left side
			children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
			if (state->child_chunk.size() == 0) {
				return;
			}
			state->child_chunk.Flatten();

			// resolve the join keys for the left chunk
			state->join_keys.Reset();
			ExpressionExecutor executor(state->child_chunk, context);
			for (size_t k = 0; k < conditions.size(); k++) {
				executor.ExecuteExpression(conditions[k].left.get(), state->join_keys.data[k]);
				// sort by join key
				OrderVector(state->join_keys.data[k], state->left_orders);
			}
			state->right_chunk_index = 0;
			state->left_position = 0;
			state->right_position = 0;
		}

		// now perform the actual merge join
		auto &right_chunk = *state->right_chunks.chunks[state->right_chunk_index];
		auto &right_condition_chunk = *state->right_conditions.chunks[state->right_chunk_index];
		auto &right_orders = state->right_orders[state->right_chunk_index];

		MergeInfo left(state->join_keys.data[0], state->left_orders.count, state->left_orders.order,
		               state->left_position);
		MergeInfo right(right_condition_chunk.data[0], right_orders.count, right_orders.order, state->right_position);
		// perform the merge join
		size_t result_count = MergeJoin(left, right, conditions[0].comparison, type);
		if (result_count == 0) {
			// exhausted this chunk on the right side
			// move to the next
			state->right_chunk_index++;
			state->left_position = 0;
			state->right_position = 0;
		} else {
			for (size_t i = 0; i < state->child_chunk.column_count; i++) {
				chunk.data[i].Reference(state->child_chunk.data[i]);
				chunk.data[i].count = result_count;
				chunk.data[i].sel_vector = left.result;
				chunk.data[i].Flatten();
			}
			// now create a reference to the chunk on the right side
			for (size_t i = 0; i < right_chunk.column_count; i++) {
				size_t chunk_entry = state->child_chunk.column_count + i;
				chunk.data[chunk_entry].Reference(right_chunk.data[i]);
				chunk.data[chunk_entry].count = result_count;
				chunk.data[chunk_entry].sel_vector = right.result;
				chunk.data[chunk_entry].Flatten();
			}
		}
	} while (chunk.size() == 0);
}

unique_ptr<PhysicalOperatorState> PhysicalPiecewiseMergeJoin::GetOperatorState(ExpressionExecutor *parent_executor) {
	return make_unique<PhysicalPiecewiseMergeJoinOperatorState>(children[0].get(), children[1].get(), parent_executor);
}
