#include "duckdb/execution/operator/join/physical_piecewise_merge_join.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/merge_join.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

PhysicalPiecewiseMergeJoin::PhysicalPiecewiseMergeJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                                       unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond,
                                                       JoinType join_type, idx_t estimated_cardinality)
    : PhysicalComparisonJoin(op, PhysicalOperatorType::PIECEWISE_MERGE_JOIN, move(cond), join_type,
                             estimated_cardinality) {
	// for now we only support one condition!
	D_ASSERT(conditions.size() == 1);
	for (auto &cond : conditions) {
		// COMPARE NOT EQUAL not supported with merge join
		D_ASSERT(cond.comparison != ExpressionType::COMPARE_NOTEQUAL);
		D_ASSERT(cond.left->return_type == cond.right->return_type);
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
	explicit MergeJoinLocalState(vector<JoinCondition> &conditions) {
		vector<LogicalType> condition_types;
		for (auto &cond : conditions) {
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
	MergeJoinGlobalState() : has_null(false), right_outer_position(0) {
	}

	//! The materialized data of the RHS
	ChunkCollection right_chunks;
	//! The materialized join keys of the RHS
	ChunkCollection right_conditions;
	//! The join orders of the RHS
	vector<MergeOrder> right_orders;
	//! Whether or not the RHS of the nested loop join has NULL values
	bool has_null;
	//! A bool indicating for each tuple in the RHS if they found a match (only used in FULL OUTER JOIN)
	unique_ptr<bool[]> right_found_match;
	//! The position in the RHS in the final scan of the FULL OUTER JOIN
	idx_t right_outer_position;
};

unique_ptr<GlobalOperatorState> PhysicalPiecewiseMergeJoin::GetGlobalState(ClientContext &context) {
	return make_unique<MergeJoinGlobalState>();
}

unique_ptr<LocalSinkState> PhysicalPiecewiseMergeJoin::GetLocalSinkState(ExecutionContext &context) {
	return make_unique<MergeJoinLocalState>(conditions);
}

void PhysicalPiecewiseMergeJoin::Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate,
                                      DataChunk &input) const {
	auto &gstate = (MergeJoinGlobalState &)state;
	auto &mj_state = (MergeJoinLocalState &)lstate;

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
	gstate.right_conditions.Append(mj_state.join_keys);
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
static void OrderVector(Vector &vector, idx_t count, MergeOrder &order);

bool PhysicalPiecewiseMergeJoin::Finalize(Pipeline &pipeline, ClientContext &context,
                                          unique_ptr<GlobalOperatorState> state) {
	auto &gstate = (MergeJoinGlobalState &)*state;
	if (gstate.right_conditions.ChunkCount() > 0) {
		// now order all the chunks
		gstate.right_orders.resize(gstate.right_conditions.ChunkCount());
		for (idx_t i = 0; i < gstate.right_conditions.ChunkCount(); i++) {
			auto &chunk_to_order = gstate.right_conditions.GetChunk(i);
			D_ASSERT(chunk_to_order.ColumnCount() == 1);
			for (idx_t col_idx = 0; col_idx < chunk_to_order.ColumnCount(); col_idx++) {
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
	if (IsRightOuterJoin(join_type)) {
		// for FULL/RIGHT OUTER JOIN, initialize found_match to false for every tuple
		gstate.right_found_match = unique_ptr<bool[]>(new bool[gstate.right_chunks.Count()]);
		memset(gstate.right_found_match.get(), 0, sizeof(bool) * gstate.right_chunks.Count());
	}
	PhysicalSink::Finalize(pipeline, context, move(state));
	return true;
}

//===--------------------------------------------------------------------===//
// GetChunkInternal
//===--------------------------------------------------------------------===//
class PhysicalPiecewiseMergeJoinState : public PhysicalOperatorState {
public:
	PhysicalPiecewiseMergeJoinState(PhysicalOperator &op, PhysicalOperator *left, vector<JoinCondition> &conditions)
	    : PhysicalOperatorState(op, left), fetch_next_left(true), left_position(0), right_position(0),
	      right_chunk_index(0) {
		vector<LogicalType> condition_types;
		for (auto &cond : conditions) {
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
	unique_ptr<bool[]> left_found_match;
};

void PhysicalPiecewiseMergeJoin::ResolveSimpleJoin(ExecutionContext &context, DataChunk &chunk,
                                                   PhysicalOperatorState *state_p) const {
	auto state = reinterpret_cast<PhysicalPiecewiseMergeJoinState *>(state_p);
	auto &gstate = (MergeJoinGlobalState &)*sink_state;
	do {
		children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
		if (state->child_chunk.size() == 0) {
			return;
		}
		state->join_keys.Reset();
		state->lhs_executor.SetChunk(state->child_chunk);
		state->join_keys.SetCardinality(state->child_chunk);
		for (idx_t k = 0; k < conditions.size(); k++) {
			state->lhs_executor.ExecuteExpression(k, state->join_keys.data[k]);
			// sort by join key
			OrderVector(state->join_keys.data[k], state->join_keys.size(), state->left_orders);
		}
		ScalarMergeInfo left_info(state->left_orders, state->join_keys.data[0].GetType(), state->left_position);
		ChunkMergeInfo right_info(gstate.right_conditions, gstate.right_orders);

		// perform the actual join
		MergeJoinSimple::Perform(left_info, right_info, conditions[0].comparison);

		// now construct the result based ont he join result
		switch (join_type) {
		case JoinType::MARK:
			PhysicalJoin::ConstructMarkJoinResult(state->join_keys, state->child_chunk, chunk, right_info.found_match,
			                                      gstate.has_null);
			break;
		case JoinType::SEMI:
			PhysicalJoin::ConstructSemiJoinResult(state->child_chunk, chunk, right_info.found_match);
			break;
		case JoinType::ANTI:
			PhysicalJoin::ConstructAntiJoinResult(state->child_chunk, chunk, right_info.found_match);
			break;
		default:
			throw NotImplementedException("Unimplemented join type for merge join");
		}
	} while (chunk.size() == 0);
}

void PhysicalPiecewiseMergeJoin::ResolveComplexJoin(ExecutionContext &context, DataChunk &chunk,
                                                    PhysicalOperatorState *state_p) const {
	auto state = reinterpret_cast<PhysicalPiecewiseMergeJoinState *>(state_p);
	auto &gstate = (MergeJoinGlobalState &)*sink_state;
	do {
		// check if we have to fetch a child from the left side
		if (state->fetch_next_left) {
			if (IsLeftOuterJoin(join_type)) {
				// left join: before we move to the next chunk, see if we need to output any vectors that didn't
				// have a match found
				if (state->left_found_match) {
					PhysicalJoin::ConstructLeftJoinResult(state->child_chunk, chunk, state->left_found_match.get());
					state->left_found_match.reset();
					if (chunk.size() > 0) {
						return;
					}
				}
				state->left_found_match = unique_ptr<bool[]>(new bool[STANDARD_VECTOR_SIZE]);
				memset(state->left_found_match.get(), 0, sizeof(bool) * STANDARD_VECTOR_SIZE);
			}
			// fetch the chunk from the left side
			children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
			if (state->child_chunk.size() == 0) {
				// exhausted LHS: in case of full outer join output remaining entries
				if (IsRightOuterJoin(join_type)) {
					// if the LHS is exhausted in a FULL OUTER JOIN, we scan the found_match for any chunks we still
					// need to output
					ConstructFullOuterJoinResult(gstate.right_found_match.get(), gstate.right_chunks, chunk,
					                             gstate.right_outer_position);
				}
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
			state->fetch_next_left = false;
		}

		auto &right_chunk = gstate.right_chunks.GetChunk(state->right_chunk_index);
		auto &right_condition_chunk = gstate.right_conditions.GetChunk(state->right_chunk_index);
		auto &right_orders = gstate.right_orders[state->right_chunk_index];

		ScalarMergeInfo left_info(state->left_orders, state->join_keys.data[0].GetType(), state->left_position);
		ScalarMergeInfo right_info(right_orders, right_condition_chunk.data[0].GetType(), state->right_position);

		idx_t result_count = MergeJoinComplex::Perform(left_info, right_info, conditions[0].comparison);
		if (result_count == 0) {
			// exhausted this chunk on the right side
			// move to the next right chunk
			state->left_position = 0;
			state->right_position = 0;
			state->right_chunk_index++;
			if (state->right_chunk_index >= gstate.right_chunks.ChunkCount()) {
				state->fetch_next_left = true;
			}
		} else {
			// found matches: mark the found matches if required
			if (state->left_found_match) {
				for (idx_t i = 0; i < result_count; i++) {
					state->left_found_match[left_info.result.get_index(i)] = true;
				}
			}
			if (gstate.right_found_match) {
				idx_t base_index = state->right_chunk_index * STANDARD_VECTOR_SIZE;
				for (idx_t i = 0; i < result_count; i++) {
					gstate.right_found_match[base_index + right_info.result.get_index(i)] = true;
				}
			}
			// found matches: output them
			chunk.Slice(state->child_chunk, left_info.result, result_count);
			chunk.Slice(right_chunk, right_info.result, result_count, state->child_chunk.ColumnCount());
		}
	} while (chunk.size() == 0);
}

void PhysicalPiecewiseMergeJoin::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
                                                  PhysicalOperatorState *state_p) const {
	auto state = reinterpret_cast<PhysicalPiecewiseMergeJoinState *>(state_p);
	auto &gstate = (MergeJoinGlobalState &)*sink_state;

	if (gstate.right_chunks.Count() == 0) {
		// empty RHS: construct empty result
		if (join_type == JoinType::SEMI || join_type == JoinType::INNER) {
			return;
		}
		children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
		if (state->child_chunk.size() == 0) {
			return;
		}
		// empty RHS: construct empty result
		ConstructEmptyJoinResult(join_type, gstate.has_null, state->child_chunk, chunk);
		return;
	}

	switch (join_type) {
	case JoinType::SEMI:
	case JoinType::ANTI:
	case JoinType::MARK:
		// simple joins can have max STANDARD_VECTOR_SIZE matches per chunk
		ResolveSimpleJoin(context, chunk, state_p);
		break;
	case JoinType::LEFT:
	case JoinType::INNER:
	case JoinType::RIGHT:
	case JoinType::OUTER:
		ResolveComplexJoin(context, chunk, state_p);
		break;
	default:
		throw NotImplementedException("Unimplemented type for nested loop join!");
	}
}

unique_ptr<PhysicalOperatorState> PhysicalPiecewiseMergeJoin::GetOperatorState() {
	return make_unique<PhysicalPiecewiseMergeJoinState>(*this, children[0].get(), conditions);
}
void PhysicalPiecewiseMergeJoin::FinalizeOperatorState(PhysicalOperatorState &state, ExecutionContext &context) {
	auto &state_p = reinterpret_cast<PhysicalPiecewiseMergeJoinState &>(state);
	context.thread.profiler.Flush(this, &state_p.lhs_executor, "lhs_executor", 0);
	if (!children.empty() && state.child_state) {
		children[0]->FinalizeOperatorState(*state.child_state, context);
	}
}
void PhysicalPiecewiseMergeJoin::Combine(ExecutionContext &context, GlobalOperatorState &gstate,
                                         LocalSinkState &lstate) {
	auto &state = (MergeJoinLocalState &)lstate;
	context.thread.profiler.Flush(this, &state.rhs_executor, "rhs_executor", 1);
	context.client.profiler->Flush(context.thread.profiler);
}

//===--------------------------------------------------------------------===//
// OrderVector
//===--------------------------------------------------------------------===//
template <class T, class OP>
static sel_t TemplatedQuicksortInitial(T *data, const SelectionVector &sel, const SelectionVector &not_null_sel,
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
	D_ASSERT(low == high);
	result.set_index(low, pivot_idx);
	return low;
}

template <class T, class OP>
static void TemplatedQuicksortRefine(T *data, const SelectionVector &sel, idx_t count, SelectionVector &result,
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
		TemplatedQuicksortRefine<T, OP>(data, sel, count, result, left, part - 1);
	}
	TemplatedQuicksortRefine<T, OP>(data, sel, count, result, part + 1, right);
}

template <class T, class OP>
void TemplatedQuicksort(T *__restrict data, const SelectionVector &sel, const SelectionVector &not_null_sel,
                        idx_t count, SelectionVector &result) {
	auto part = TemplatedQuicksortInitial<T, OP>(data, sel, not_null_sel, count, result);
	if (part > count) {
		return;
	}
	TemplatedQuicksortRefine<T, OP>(data, sel, count, result, 0, part);
	TemplatedQuicksortRefine<T, OP>(data, sel, count, result, part + 1, count - 1);
}

template <class T>
static void TemplatedQuicksort(VectorData &vdata, const SelectionVector &not_null_sel, idx_t not_null_count,
                               SelectionVector &result) {
	if (not_null_count == 0) {
		return;
	}
	TemplatedQuicksort<T, duckdb::LessThanEquals>((T *)vdata.data, *vdata.sel, not_null_sel, not_null_count, result);
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
		if (vdata.validity.RowIsValid(idx)) {
			not_null.set_index(not_null_count++, i);
		}
	}

	order.count = not_null_count;
	order.order.Initialize(STANDARD_VECTOR_SIZE);
	switch (vector.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedQuicksort<int8_t>(vdata, not_null, not_null_count, order.order);
		break;
	case PhysicalType::INT16:
		TemplatedQuicksort<int16_t>(vdata, not_null, not_null_count, order.order);
		break;
	case PhysicalType::INT32:
		TemplatedQuicksort<int32_t>(vdata, not_null, not_null_count, order.order);
		break;
	case PhysicalType::INT64:
		TemplatedQuicksort<int64_t>(vdata, not_null, not_null_count, order.order);
		break;
	case PhysicalType::UINT8:
		TemplatedQuicksort<uint8_t>(vdata, not_null, not_null_count, order.order);
		break;
	case PhysicalType::UINT16:
		TemplatedQuicksort<uint16_t>(vdata, not_null, not_null_count, order.order);
		break;
	case PhysicalType::UINT32:
		TemplatedQuicksort<uint32_t>(vdata, not_null, not_null_count, order.order);
		break;
	case PhysicalType::UINT64:
		TemplatedQuicksort<uint64_t>(vdata, not_null, not_null_count, order.order);
		break;
	case PhysicalType::INT128:
		TemplatedQuicksort<hugeint_t>(vdata, not_null, not_null_count, order.order);
		break;
	case PhysicalType::FLOAT:
		TemplatedQuicksort<float>(vdata, not_null, not_null_count, order.order);
		break;
	case PhysicalType::DOUBLE:
		TemplatedQuicksort<double>(vdata, not_null, not_null_count, order.order);
		break;
	case PhysicalType::INTERVAL:
		TemplatedQuicksort<interval_t>(vdata, not_null, not_null_count, order.order);
		break;
	case PhysicalType::VARCHAR:
		TemplatedQuicksort<string_t>(vdata, not_null, not_null_count, order.order);
		break;
	default:
		throw NotImplementedException("Unimplemented type for sort");
	}
}

} // namespace duckdb
