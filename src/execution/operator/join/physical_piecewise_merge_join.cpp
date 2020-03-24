#include "duckdb/execution/operator/join/physical_piecewise_merge_join.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/merge_join.hpp"

using namespace duckdb;
using namespace std;

static void OrderVector(Vector &vector, idx_t count, MergeOrder &order);

class PhysicalPiecewiseMergeJoinState : public PhysicalComparisonJoinState {
public:
	PhysicalPiecewiseMergeJoinState(PhysicalOperator *left, PhysicalOperator *right, vector<JoinCondition> &conditions)
	    : PhysicalComparisonJoinState(left, right, conditions), initialized(false), left_position(0), right_position(0),
	      right_chunk_index(0), has_null(false) {
	}

	bool initialized;
	idx_t left_position;
	idx_t right_position;
	idx_t right_chunk_index;
	DataChunk left_chunk;
	DataChunk join_keys;
	MergeOrder left_orders;
	ChunkCollection right_chunks;
	ChunkCollection right_conditions;
	vector<MergeOrder> right_orders;
	bool has_null;
};

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

void PhysicalPiecewiseMergeJoin::GetChunkInternal(ClientContext &context, DataChunk &chunk,
                                                  PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalPiecewiseMergeJoinState *>(state_);
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
		for (idx_t i = 0; i < state->right_chunks.chunks.size(); i++) {
			auto &chunk_to_order = *state->right_chunks.chunks[i];
			// create a new selection vector
			// resolve the join keys for the right chunk
			state->join_keys.Reset();
			state->rhs_executor.SetChunk(chunk_to_order);

			state->join_keys.SetCardinality(chunk_to_order);
			for (idx_t k = 0; k < conditions.size(); k++) {
				// resolve the join key
				state->rhs_executor.ExecuteExpression(k, state->join_keys.data[k]);
				OrderVector(state->join_keys.data[k], state->join_keys.size(), state->right_orders[i]);
				if (state->right_orders[i].count < state->join_keys.size()) {
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

			// resolve the join keys for the left chunk
			state->join_keys.Reset();
			state->lhs_executor.SetChunk(state->child_chunk);
			state->join_keys.SetCardinality(state->child_chunk);
			for (idx_t k = 0; k < conditions.size(); k++) {
				state->lhs_executor.ExecuteExpression(k, state->join_keys.data[k]);
				// sort by join key
				OrderVector(state->join_keys.data[k], state->join_keys.size(), state->left_orders);
			}
			state->right_chunk_index = 0;
			state->left_position = 0;
			state->right_position = 0;
		}

		ScalarMergeInfo left_info(state->left_orders, state->join_keys.data[0].type, state->left_position);

		// first check if the join type is MARK, SEMI or ANTI
		// in this case we loop over the entire right collection immediately
		// because we can never return more than STANDARD_VECTOR_SIZE rows from a join
		switch (type) {
		case JoinType::MARK: {
			throw NotImplementedException("FIXME mark join");
			// // MARK join
			// if (state->right_chunks.count > 0) {
			// 	ChunkMergeInfo right_info(state->right_conditions, state->right_orders);
			// 	throw NotImplementedException("FIXME mark join");
			// 	// first perform the MARK join
			// 	// this method uses the LHS to loop over the entire RHS looking for matches
			// 	// MergeJoinMark::Perform(left_info, right_info, conditions[0].comparison);
			// 	// // now construct the mark join result from the found matches
			// 	// ConstructMarkJoinResult(state->join_keys, state->child_chunk, chunk, right_info.found_match,
			// 	//                         state->has_null);
			// 	// move to the next LHS chunk in the next iteration
			// } else {
			// 	// RHS empty: just set found_match to false
			// 	bool found_match[STANDARD_VECTOR_SIZE] = {false};
			// 	ConstructMarkJoinResult(state->join_keys, state->child_chunk, chunk, found_match, state->has_null);
			// 	// RHS empty: result is not NULL but just false
			// 	chunk.data[chunk.column_count() - 1].nullmask.reset();
			// }
			// state->right_chunk_index = state->right_orders.size();
			// return;
		}
		default:
			// INNER, LEFT OUTER, etc... join that can return >STANDARD_VECTOR_SIZE entries
			break;
		}

		// perform the actual merge join
		auto &right_chunk = *state->right_chunks.chunks[state->right_chunk_index];
		auto &right_condition_chunk = *state->right_conditions.chunks[state->right_chunk_index];
		auto &right_orders = state->right_orders[state->right_chunk_index];

		ScalarMergeInfo right(right_orders, right_condition_chunk.data[0].type, state->right_position);
		// perform the merge join
		switch (type) {
		case JoinType::INNER: {
			idx_t result_count = MergeJoinInner::Perform(left_info, right, conditions[0].comparison);
			if (result_count == 0) {
				// exhausted this chunk on the right side
				// move to the next
				state->right_chunk_index++;
				state->left_position = 0;
				state->right_position = 0;
			} else {
				chunk.Slice(state->child_chunk, left_info.result, result_count);
				chunk.Slice(right_chunk, right.result, result_count, state->child_chunk.column_count());
			}
			break;
		}
		default:
			throw NotImplementedException("Unimplemented join type for merge join");
		}
	} while (chunk.size() == 0);
}

unique_ptr<PhysicalOperatorState> PhysicalPiecewiseMergeJoin::GetOperatorState() {
	return make_unique<PhysicalPiecewiseMergeJoinState>(children[0].get(), children[1].get(), conditions);
}

template <class T, class OP>
static sel_t templated_quicksort_initial(T *data, const SelectionVector &sel, idx_t count, SelectionVector &result) {
	// select pivot
	sel_t pivot = 0;
	sel_t low = 0, high = count - 1;
	// now insert elements
	for (idx_t i = 1; i < count; i++) {
		if (OP::Operation(data[sel.get_index(i)], data[sel.get_index(pivot)])) {
			result.set_index(low++, sel.get_index(i));
		} else {
			result.set_index(high--, sel.get_index(i));
		}
	}
	assert(low == high);
	result.set_index(low, sel.get_index(pivot));
	return low;
}

template <class T, class OP>
static void templated_quicksort_inplace(T *data, idx_t count, SelectionVector &result, sel_t left, sel_t right) {
	if (left >= right) {
		return;
	}

	sel_t middle = left + (right - left) / 2;
	sel_t pivot = result.get_index(middle);

	// move the mid point value to the front.
	sel_t i = left + 1;
	sel_t j = right;

	result.swap(middle, left);
	while (i <= j) {
		while (i <= j && (OP::Operation(data[result.get_index(i)], data[pivot]))) {
			i++;
		}

		while (i <= j && OP::Operation(data[pivot], data[result.get_index(j)])) {
			j--;
		}

		if (i < j) {
			result.swap(i, j);
		}
	}
	result.swap(i - 1, left);
	sel_t part = i - 1;

	if (part > 0) {
		templated_quicksort_inplace<T, OP>(data, count, result, left, part - 1);
	}
	templated_quicksort_inplace<T, OP>(data, count, result, part + 1, right);
}

template <class T, class OP> void templated_quicksort(T *__restrict data, const SelectionVector &sel, idx_t count, SelectionVector &result) {
	auto part = templated_quicksort_initial<T, OP>(data, sel, count, result);
	if (part > count) {
		return;
	}
	templated_quicksort_inplace<T, OP>(data, count, result, 0, part);
	templated_quicksort_inplace<T, OP>(data, count, result, part + 1, count - 1);
}

template <class T>
static void templated_quicksort(data_ptr_t data, const SelectionVector &sel, idx_t count, SelectionVector &result) {
	templated_quicksort<T, duckdb::LessThanEquals>((T*) data, sel, count, result);
}

void OrderVector(Vector &vector, idx_t count, MergeOrder &order) {
	if (count == 0) {
		order.count = 0;
		return;
	}
	vector.Orrify(count, order.vdata);
	auto &vdata = order.vdata;

	// first filter out all the non-null values
	idx_t not_null_count = 0;
	SelectionVector not_null(STANDARD_VECTOR_SIZE);
	for(idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if (!(*vdata.nullmask)[i]) {
			not_null.set_index(not_null_count++, idx);
		}
	}
	order.count = not_null_count;

	auto dataptr = order.vdata.data;
	order.order.Initialize(STANDARD_VECTOR_SIZE);
	switch (vector.type) {
	case TypeId::INT8:
		templated_quicksort<int8_t>(dataptr, not_null, not_null_count, order.order);
		break;
	case TypeId::INT16:
		templated_quicksort<int16_t>(dataptr, not_null, not_null_count, order.order);
		break;
	case TypeId::INT32:
		templated_quicksort<int32_t>(dataptr, not_null, not_null_count, order.order);
		break;
	case TypeId::INT64:
		templated_quicksort<int64_t>(dataptr, not_null, not_null_count, order.order);
		break;
	case TypeId::FLOAT:
		templated_quicksort<float>(dataptr, not_null, not_null_count, order.order);
		break;
	case TypeId::DOUBLE:
		templated_quicksort<double>(dataptr, not_null, not_null_count, order.order);
		break;
	case TypeId::VARCHAR:
		templated_quicksort<string_t>(dataptr, not_null, not_null_count, order.order);
		break;
	default:
		throw NotImplementedException("Unimplemented type for sort");
	}
}
