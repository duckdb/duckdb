#include "execution/operator/join/physical_piecewise_merge_join.hpp"

#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"
#include "execution/merge_join.hpp"

using namespace duckdb;
using namespace std;

PhysicalPiecewiseMergeJoin::PhysicalPiecewiseMergeJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                                       unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond,
                                                       JoinType join_type)
    : PhysicalComparisonJoin(op, PhysicalOperatorType::PIECEWISE_MERGE_JOIN, move(cond), join_type) {
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

//! Create the order vector of a specific vector,
static void OrderVector(Vector &vector, MergeOrder &order) {
	// first remove any NULL values; they can never match anyway
	sel_t not_null_order[STANDARD_VECTOR_SIZE];
	sel_t *result_vector;
	order.count = Vector::NotNullSelVector(vector, not_null_order, result_vector);
	// sort by the join key
	VectorOperations::Sort(vector, result_vector, order.count, order.order);
}

void PhysicalPiecewiseMergeJoin::GetChunkInternal(ClientContext &context, DataChunk &chunk,
                                                  PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalPiecewiseMergeJoinOperatorState *>(state_);
	assert(conditions.size() == 1);
	if (!state->initialized) {
		// create the sorted pieces
		auto right_state = children[1]->GetOperatorState();
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
		if (state->right_chunks.count == 0 && (type == JoinType::INNER || type == JoinType::SEMI)) {
			// empty RHS with INNER or SEMI join means empty result set
			return;
		}
		// now order all the chunks
		state->right_orders.resize(state->right_chunks.chunks.size());
		for (index_t i = 0; i < state->right_chunks.chunks.size(); i++) {
			auto &chunk_to_order = *state->right_chunks.chunks[i];
			// create a new selection vector
			// resolve the join keys for the right chunk
			state->join_keys.Reset();
			ExpressionExecutor executor(chunk_to_order);
			for (index_t k = 0; k < conditions.size(); k++) {
				// resolve the join key
				executor.ExecuteExpression(*conditions[k].right, state->join_keys.data[k]);
				OrderVector(state->join_keys.data[k], state->right_orders[i]);
				if (state->right_orders[i].count < state->join_keys.data[k].count) {
					// the amount of entries in the order vector is smaller than the amount of entries in the vector
					// this only happens if there are NULL values in the right-hand side
					// hence we set the has_null to true (this is required for the MARK join)
					state->has_null = true;
				}
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
			ExpressionExecutor executor(state->child_chunk);
			for (index_t k = 0; k < conditions.size(); k++) {
				executor.ExecuteExpression(*conditions[k].left, state->join_keys.data[k]);
				// sort by join key
				OrderVector(state->join_keys.data[k], state->left_orders);
			}
			state->right_chunk_index = 0;
			state->left_position = 0;
			state->right_position = 0;
		}

		ScalarMergeInfo left_info(state->join_keys.data[0], state->left_orders.count, state->left_orders.order,
		                          state->left_position);

		// first check if the join type is MARK, SEMI or ANTI
		// in this case we loop over the entire right collection immediately
		// because we can never return more than STANDARD_VECTOR_SIZE rows from a join
		switch (type) {
		case JoinType::MARK: {
			// MARK join
			if (state->right_chunks.count > 0) {
				ChunkMergeInfo right_info(state->right_conditions, state->right_orders);
				// first perform the MARK join
				// this method uses the LHS to loop over the entire RHS looking for matches
				MergeJoinMark::Perform(left_info, right_info, conditions[0].comparison);
				// now construct the mark join result from the found matches
				ConstructMarkJoinResult(state->join_keys, state->child_chunk, chunk, right_info.found_match,
				                        state->has_null);
				// move to the next LHS chunk in the next iteration
			} else {
				// RHS empty: just set found_match to false
				bool found_match[STANDARD_VECTOR_SIZE] = {false};
				ConstructMarkJoinResult(state->join_keys, state->child_chunk, chunk, found_match, state->has_null);
				// RHS empty: result is not NULL but just false
				chunk.data[chunk.column_count - 1].nullmask.reset();
			}
			state->right_chunk_index = state->right_orders.size();
			return;
		}
		default:
			// INNER, LEFT OUTER, etc... join that can return >STANDARD_VECTOR_SIZE entries
			break;
		}

		// perform the actual merge join
		auto &right_chunk = *state->right_chunks.chunks[state->right_chunk_index];
		auto &right_condition_chunk = *state->right_conditions.chunks[state->right_chunk_index];
		auto &right_orders = state->right_orders[state->right_chunk_index];

		ScalarMergeInfo right(right_condition_chunk.data[0], right_orders.count, right_orders.order,
		                      state->right_position);
		// perform the merge join
		switch (type) {
		case JoinType::INNER: {
			index_t result_count = MergeJoinInner::Perform(left_info, right, conditions[0].comparison);
			if (result_count == 0) {
				// exhausted this chunk on the right side
				// move to the next
				state->right_chunk_index++;
				state->left_position = 0;
				state->right_position = 0;
			} else {
				for (index_t i = 0; i < state->child_chunk.column_count; i++) {
					chunk.data[i].Reference(state->child_chunk.data[i]);
					chunk.data[i].count = result_count;
					chunk.data[i].sel_vector = left_info.result;
					chunk.data[i].Flatten();
				}
				// now create a reference to the chunk on the right side
				for (index_t i = 0; i < right_chunk.column_count; i++) {
					index_t chunk_entry = state->child_chunk.column_count + i;
					chunk.data[chunk_entry].Reference(right_chunk.data[i]);
					chunk.data[chunk_entry].count = result_count;
					chunk.data[chunk_entry].sel_vector = right.result;
					chunk.data[chunk_entry].Flatten();
				}
			}
			break;
		}
		default:
			throw NotImplementedException("Unimplemented join type for merge join");
		}
	} while (chunk.size() == 0);
}

unique_ptr<PhysicalOperatorState> PhysicalPiecewiseMergeJoin::GetOperatorState() {
	return make_unique<PhysicalPiecewiseMergeJoinOperatorState>(children[0].get(), children[1].get());
}
