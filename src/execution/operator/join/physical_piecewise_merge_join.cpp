#include "duckdb/execution/operator/join/physical_piecewise_merge_join.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/merge_join.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"

using namespace std;

namespace duckdb {

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

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class MergeJoinLocalState : public LocalSinkState {
public:
	MergeJoinLocalState(vector<JoinCondition> &conditions) {
		vector<TypeId> condition_types;
		for(auto &cond : conditions) {
			rhs_executor.AddExpression(*cond.right);
			condition_types.push_back(cond.right->return_type);
		}
		join_keys.Initialize(condition_types);
	}

	//! The chunk holding the right condition
	DataChunk join_keys;
	//! The executor of the RHS condition
	ExpressionExecutor rhs_executor;
};

class MergeJoinGlobalState : public GlobalOperatorState {
public:
	MergeJoinGlobalState() : has_null(false) {
	}

	//! The materialized data of the RHS
	ChunkCollection right_chunks;
	//! The materialized join keys of the RHS
	ChunkCollection right_conditions;
	//! The join orders of the RHS
	vector<MergeOrder> right_orders;
	//! Whether or not the RHS of the nested loop join has NULL values
	bool has_null;
};

unique_ptr<GlobalOperatorState> PhysicalPiecewiseMergeJoin::GetGlobalState(ClientContext &context) {
	return make_unique<MergeJoinGlobalState>();
}

unique_ptr<LocalSinkState> PhysicalPiecewiseMergeJoin::GetLocalSinkState(ClientContext &context) {
	return make_unique<MergeJoinLocalState>(conditions);
}

void PhysicalPiecewiseMergeJoin::Sink(ClientContext &context, GlobalOperatorState &state, LocalSinkState &lstate, DataChunk &input) {
	auto &gstate = (MergeJoinGlobalState &) state;
	auto &mj_state = (MergeJoinLocalState &) lstate;

	// resolve the join keys for this chunk
	mj_state.rhs_executor.SetChunk(input);

	mj_state.join_keys.Reset();
	mj_state.join_keys.SetCardinality(input);
	for (idx_t k = 0; k < conditions.size(); k++) {
		// resolve the join key
		mj_state.rhs_executor.ExecuteExpression(k, mj_state.join_keys.data[k]);
	}
	// append the join keys and the chunk to the chunk collection
	gstate.right_chunks.Append(input);
	gstate.right_conditions.Append(mj_state.join_keys);}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
static void OrderVector(Vector &vector, idx_t count, MergeOrder &order);

void PhysicalPiecewiseMergeJoin::Finalize(ClientContext &context, GlobalOperatorState &state) {
	auto &gstate = (MergeJoinGlobalState &) state;
	if (gstate.right_conditions.chunks.size() == 0) {
		return;
	}
	// now order all the chunks
	gstate.right_orders.resize(gstate.right_conditions.chunks.size());
	for (idx_t i = 0; i < gstate.right_conditions.chunks.size(); i++) {
		auto &chunk_to_order = *gstate.right_conditions.chunks[i];
		assert(chunk_to_order.column_count() == 1);
		for (idx_t col_idx = 0; col_idx < chunk_to_order.column_count(); col_idx++) {
			OrderVector(chunk_to_order.data[col_idx], chunk_to_order.size(), gstate.right_orders[i]);
			if (gstate.right_orders[i].count < chunk_to_order.size()) {
				// the amount of entries in the order vector is smaller than the amount of entries in the vector
				// this only happens if there are NULL values in the right-hand side
				// hence we set the has_null to true (this is required for the MARK join)
				gstate.has_null = true;
			}
		}
	}
}

//===--------------------------------------------------------------------===//
// GetChunkInternal
//===--------------------------------------------------------------------===//
class PhysicalPiecewiseMergeJoinState : public PhysicalOperatorState {
public:
	PhysicalPiecewiseMergeJoinState(PhysicalOperator *left, vector<JoinCondition> &conditions)
	    : PhysicalOperatorState(left), fetch_next_left(true), left_position(0), right_position(0),
	      right_chunk_index(0) {
		vector<TypeId> condition_types;
		for(auto &cond : conditions) {
			lhs_executor.AddExpression(*cond.left);
			condition_types.push_back(cond.left->return_type);
		}
		join_keys.Initialize(condition_types);
	}

	bool fetch_next_left;
	idx_t left_position;
	idx_t right_position;
	idx_t right_chunk_index;
	DataChunk left_chunk;
	DataChunk join_keys;
	MergeOrder left_orders;
	//! The executor of the RHS condition
	ExpressionExecutor lhs_executor;
};

void PhysicalPiecewiseMergeJoin::GetChunkInternal(ClientContext &context, DataChunk &chunk,
                                                  PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalPiecewiseMergeJoinState *>(state_);
	auto &gstate = (MergeJoinGlobalState &) *sink_state;

	do {
		// check if we have to fetch a child from the left side
		if (state->fetch_next_left) {
			// fetch the chunk from the left side
			children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
			if (state->child_chunk.size() == 0) {
				return;
			}
			if (gstate.right_chunks.count == 0) {
				// empty RHS: construct empty result
				ConstructEmptyJoinResult(type, gstate.has_null, state->child_chunk, chunk);
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

		// check if the join type is MARK, SEMI or ANTI
		// in this case we loop over the entire right collection immediately
		// because we can never return more than STANDARD_VECTOR_SIZE rows from a join
		switch (type) {
		case JoinType::MARK: {
			// MARK join
			ChunkMergeInfo right_info(gstate.right_conditions, gstate.right_orders);
			// first perform the MARK join
			// this method uses the LHS to loop over the entire RHS looking for matches
			MergeJoinMark::Perform(left_info, right_info, conditions[0].comparison);
			// now construct the mark join result from the found matches
			PhysicalJoin::ConstructMarkJoinResult(state->join_keys, state->child_chunk, chunk,
													right_info.found_match, gstate.has_null);
			state->right_chunk_index = gstate.right_orders.size();
			return;
		}
		default:
			// INNER, LEFT OUTER, etc... join that can return >STANDARD_VECTOR_SIZE entries
			break;
		}

		// perform the actual merge join
		auto &right_chunk = *gstate.right_chunks.chunks[state->right_chunk_index];
		auto &right_condition_chunk = *gstate.right_conditions.chunks[state->right_chunk_index];
		auto &right_orders = gstate.right_orders[state->right_chunk_index];

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
	} while(chunk.size() == 0);
}

unique_ptr<PhysicalOperatorState> PhysicalPiecewiseMergeJoin::GetOperatorState() {
	return make_unique<PhysicalPiecewiseMergeJoinState>(children[0].get(), conditions);
}

//===--------------------------------------------------------------------===//
// OrderVector
//===--------------------------------------------------------------------===//
template <class T, class OP>
static sel_t templated_quicksort_initial(T *data, const SelectionVector &sel, const SelectionVector &not_null_sel,
                                         idx_t count, SelectionVector &result) {
	// select pivot
	auto pivot_idx = not_null_sel.get_index(0);
	auto dpivot_idx = sel.get_index(pivot_idx);
	sel_t low = 0, high = count - 1;
	// now insert elements
	for (idx_t i = 1; i < count; i++) {
		auto idx = not_null_sel.get_index(i);
		auto didx = sel.get_index(idx);
		if (OP::Operation(data[didx], data[dpivot_idx])) {
			result.set_index(low++, idx);
		} else {
			result.set_index(high--, idx);
		}
	}
	assert(low == high);
	result.set_index(low, pivot_idx);
	return low;
}

template <class T, class OP>
static void templated_quicksort_inplace(T *data, const SelectionVector &sel, idx_t count, SelectionVector &result,
                                        sel_t left, sel_t right) {
	if (left >= right) {
		return;
	}

	sel_t middle = left + (right - left) / 2;
	sel_t dpivot_idx = sel.get_index(result.get_index(middle));

	// move the mid point value to the front.
	sel_t i = left + 1;
	sel_t j = right;

	result.swap(middle, left);
	while (i <= j) {
		while (i <= j && (OP::Operation(data[sel.get_index(result.get_index(i))], data[dpivot_idx]))) {
			i++;
		}

		while (i <= j && !OP::Operation(data[sel.get_index(result.get_index(j))], data[dpivot_idx])) {
			j--;
		}

		if (i < j) {
			result.swap(i, j);
		}
	}
	result.swap(i - 1, left);
	sel_t part = i - 1;

	if (part > 0) {
		templated_quicksort_inplace<T, OP>(data, sel, count, result, left, part - 1);
	}
	templated_quicksort_inplace<T, OP>(data, sel, count, result, part + 1, right);
}

template <class T, class OP>
void templated_quicksort(T *__restrict data, const SelectionVector &sel, const SelectionVector &not_null_sel,
                         idx_t count, SelectionVector &result) {
	auto part = templated_quicksort_initial<T, OP>(data, sel, not_null_sel, count, result);
	if (part > count) {
		return;
	}
	templated_quicksort_inplace<T, OP>(data, sel, count, result, 0, part);
	templated_quicksort_inplace<T, OP>(data, sel, count, result, part + 1, count - 1);
}

template <class T>
static void templated_quicksort(VectorData &vdata, const SelectionVector &not_null_sel, idx_t not_null_count,
                                SelectionVector &result) {
	if (not_null_count == 0) {
		return;
	}
	templated_quicksort<T, duckdb::LessThanEquals>((T *)vdata.data, *vdata.sel, not_null_sel, not_null_count, result);
}

void OrderVector(Vector &vector, idx_t count, MergeOrder &order) {
	if (count == 0) {
		order.count = 0;
		return;
	}
	vector.Orrify(count, order.vdata);
	auto &vdata = order.vdata;

	// first filter out all the non-null values
	SelectionVector not_null(STANDARD_VECTOR_SIZE);
	idx_t not_null_count = 0;
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if (!(*vdata.nullmask)[idx]) {
			not_null.set_index(not_null_count++, i);
		}
	}

	order.count = not_null_count;
	order.order.Initialize(STANDARD_VECTOR_SIZE);
	switch (vector.type) {
	case TypeId::BOOL:
	case TypeId::INT8:
		templated_quicksort<int8_t>(vdata, not_null, not_null_count, order.order);
		break;
	case TypeId::INT16:
		templated_quicksort<int16_t>(vdata, not_null, not_null_count, order.order);
		break;
	case TypeId::INT32:
		templated_quicksort<int32_t>(vdata, not_null, not_null_count, order.order);
		break;
	case TypeId::INT64:
		templated_quicksort<int64_t>(vdata, not_null, not_null_count, order.order);
		break;
	case TypeId::FLOAT:
		templated_quicksort<float>(vdata, not_null, not_null_count, order.order);
		break;
	case TypeId::DOUBLE:
		templated_quicksort<double>(vdata, not_null, not_null_count, order.order);
		break;
	case TypeId::VARCHAR:
		templated_quicksort<string_t>(vdata, not_null, not_null_count, order.order);
		break;
	default:
		throw NotImplementedException("Unimplemented type for sort");
	}
}

}
