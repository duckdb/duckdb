
#include "execution/operator/physical_nested_loop_join_inner.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"

#include "common/types/constant_vector.hpp"
#include "common/types/static_vector.hpp"

#include "common/operator/comparison_operators.hpp"

using namespace std;

namespace duckdb {

PhysicalNestedLoopJoinInner::PhysicalNestedLoopJoinInner(
    unique_ptr<PhysicalOperator> left, unique_ptr<PhysicalOperator> right,
    vector<JoinCondition> cond, JoinType join_type)
    : PhysicalJoin(PhysicalOperatorType::NESTED_LOOP_JOIN, move(cond),
                   join_type) {
	children.push_back(move(left));
	children.push_back(move(right));
}

template <class T, class OP>
static size_t templated_nested_loop_join(Vector &left, Vector &right,
                                         size_t &lpos, size_t &rpos,
                                         sel_t lvector[], sel_t rvector[]) {
	auto ldata = (T *)left.data;
	auto rdata = (T *)right.data;
	size_t result_count = 0;
	while (true) {
		size_t left_position = left.sel_vector ? left.sel_vector[lpos] : lpos;
		if (OP::Operation(ldata[left_position], rdata[rpos])) {
			// emit tuple
			lvector[result_count] = left_position;
			rvector[result_count] = rpos;
			result_count++;
			if (result_count == STANDARD_VECTOR_SIZE) {
				// out of space!
				break;
			}
		}
		// move to next tuple
		lpos++;
		if (lpos == left.count) {
			lpos = 0;
			rpos++;
			if (rpos == right.count) {
				// exhausted all tuples
				break;
			}
		}
	}
	return result_count;
}

template <class OP>
static size_t nested_loop_join_operator(Vector &left, Vector &right,
                                        size_t &lpos, size_t &rpos,
                                        sel_t lvector[], sel_t rvector[]) {
	assert(left.type == right.type);
	switch (left.type) {
	case TypeId::BOOLEAN:
	case TypeId::TINYINT:
		return templated_nested_loop_join<int8_t, OP>(left, right, lpos, rpos,
		                                              lvector, rvector);
	case TypeId::SMALLINT:
		return templated_nested_loop_join<int16_t, OP>(left, right, lpos, rpos,
		                                               lvector, rvector);
	case TypeId::DATE:
	case TypeId::INTEGER:
		return templated_nested_loop_join<int32_t, OP>(left, right, lpos, rpos,
		                                               lvector, rvector);
	case TypeId::TIMESTAMP:
	case TypeId::BIGINT:
		return templated_nested_loop_join<int64_t, OP>(left, right, lpos, rpos,
		                                               lvector, rvector);
	case TypeId::DECIMAL:
		return templated_nested_loop_join<double, OP>(left, right, lpos, rpos,
		                                              lvector, rvector);
	case TypeId::POINTER:
		return templated_nested_loop_join<uint64_t, OP>(left, right, lpos, rpos,
		                                                lvector, rvector);
	case TypeId::VARCHAR:
		return templated_nested_loop_join<const char*, OP>(left, right, lpos, rpos,
		                                                lvector, rvector);
	default:
		throw NotImplementedException("Unimplemented type for join!");
	}
}

size_t nested_loop_join(ExpressionType op, Vector &left, Vector &right,
                        size_t &lpos, size_t &rpos, sel_t lvector[],
                        sel_t rvector[]) {
	if (lpos >= left.count || rpos >= right.count) {
		return 0;
	}
	// should be flattened before
	assert(!right.sel_vector);
	switch (op) {
	case ExpressionType::COMPARE_EQUAL:
		return nested_loop_join_operator<operators::Equals>(
		    left, right, lpos, rpos, lvector, rvector);
	case ExpressionType::COMPARE_NOTEQUAL:
		return nested_loop_join_operator<operators::NotEquals>(
		    left, right, lpos, rpos, lvector, rvector);
	case ExpressionType::COMPARE_LESSTHAN:
		return nested_loop_join_operator<operators::LessThan>(
		    left, right, lpos, rpos, lvector, rvector);
	case ExpressionType::COMPARE_GREATERTHAN:
		return nested_loop_join_operator<operators::GreaterThan>(
		    left, right, lpos, rpos, lvector, rvector);
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return nested_loop_join_operator<operators::LessThanEquals>(
		    left, right, lpos, rpos, lvector, rvector);
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return nested_loop_join_operator<operators::GreaterThanEquals>(
		    left, right, lpos, rpos, lvector, rvector);
	default:
		throw NotImplementedException(
		    "Unimplemented comparison type for join!");
	}
}

template <class T, class OP>
static size_t templated_nested_loop_comparison(Vector &left, Vector &right,
                                               sel_t lvector[], sel_t rvector[],
                                               size_t count) {
	assert(count > 0);

	// should be flattened before
	assert(!left.sel_vector && !right.sel_vector);
	auto ldata = (T *)left.data;
	auto rdata = (T *)right.data;
	size_t result_count = 0;
	for (size_t i = 0; i < count; i++) {
		if (OP::Operation(ldata[lvector[i]], rdata[rvector[i]])) {
			lvector[result_count] = lvector[i];
			rvector[result_count] = rvector[i];
			result_count++;
		}
	}
	return result_count;
}

template <class OP>
static size_t nested_loop_comparison_operator(Vector &l, Vector &r,
                                              sel_t lvector[], sel_t rvector[],
                                              size_t count) {
	assert(l.type == r.type);
	switch (l.type) {
	case TypeId::BOOLEAN:
	case TypeId::TINYINT:
		return templated_nested_loop_comparison<int8_t, OP>(l, r, lvector,
		                                                    rvector, count);
	case TypeId::SMALLINT:
		return templated_nested_loop_comparison<int16_t, OP>(l, r, lvector,
		                                                     rvector, count);
	case TypeId::DATE:
	case TypeId::INTEGER:
		return templated_nested_loop_comparison<int32_t, OP>(l, r, lvector,
		                                                     rvector, count);
	case TypeId::TIMESTAMP:
	case TypeId::BIGINT:
		return templated_nested_loop_comparison<int64_t, OP>(l, r, lvector,
		                                                     rvector, count);
	case TypeId::DECIMAL:
		return templated_nested_loop_comparison<double, OP>(l, r, lvector,
		                                                    rvector, count);
	case TypeId::POINTER:
		return templated_nested_loop_comparison<uint64_t, OP>(l, r, lvector,
		                                                      rvector, count);
	case TypeId::VARCHAR:
		return templated_nested_loop_comparison<const char*, OP>(l, r, lvector,
		                                                      rvector, count);
	default:
		throw NotImplementedException("Unimplemented type for join!");
	}
}

size_t nested_loop_comparison(ExpressionType op, Vector &l, Vector &r,
                              sel_t lvector[], sel_t rvector[], size_t count) {
	switch (op) {
	case ExpressionType::COMPARE_EQUAL:
		return nested_loop_comparison_operator<operators::Equals>(
		    l, r, lvector, rvector, count);
	case ExpressionType::COMPARE_NOTEQUAL:
		return nested_loop_comparison_operator<operators::NotEquals>(
		    l, r, lvector, rvector, count);
	case ExpressionType::COMPARE_LESSTHAN:
		return nested_loop_comparison_operator<operators::LessThan>(
		    l, r, lvector, rvector, count);
	case ExpressionType::COMPARE_GREATERTHAN:
		return nested_loop_comparison_operator<operators::GreaterThan>(
		    l, r, lvector, rvector, count);
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return nested_loop_comparison_operator<operators::LessThanEquals>(
		    l, r, lvector, rvector, count);
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return nested_loop_comparison_operator<operators::GreaterThanEquals>(
		    l, r, lvector, rvector, count);
	default:
		throw NotImplementedException(
		    "Unimplemented comparison type for join!");
	}
}

void PhysicalNestedLoopJoinInner::_GetChunk(ClientContext &context,
                                            DataChunk &chunk,
                                            PhysicalOperatorState *state_) {
	assert(type == JoinType::INNER);

	auto state =
	    reinterpret_cast<PhysicalNestedLoopJoinInnerOperatorState *>(state_);
	chunk.Reset();

	// first we fully materialize the right child, if we haven't done that yet
	if (state->right_chunks.column_count() == 0) {
		vector<TypeId> condition_types;
		for (auto &cond : conditions) {
			assert(cond.left->return_type == cond.right->return_type);
			condition_types.push_back(cond.left->return_type);
		}

		auto right_state = children[1]->GetOperatorState(state->parent);
		auto types = children[1]->GetTypes();

		DataChunk new_chunk, right_condition;
		new_chunk.Initialize(types);
		right_condition.Initialize(condition_types);
		do {
			children[1]->GetChunk(context, new_chunk, right_state.get());
			if (new_chunk.size() == 0) {
				break;
			}
			// resolve the join expression of the right side
			ExpressionExecutor executor(new_chunk, context);
			for (size_t i = 0; i < conditions.size(); i++) {
				executor.ExecuteExpression(conditions[i].right.get(),
				                           right_condition.data[i]);
			}
			state->right_data.Append(new_chunk);
			state->right_chunks.Append(right_condition);
		} while (new_chunk.size() > 0);

		if (state->right_chunks.count == 0) {
			return;
		}
		// initialize the chunks for the join conditions
		state->left_join_condition.Initialize(condition_types);
		state->right_chunk = state->right_chunks.chunks.size() - 1;
		state->right_tuple =
		    state->right_chunks.chunks[state->right_chunk]->size();
	}

	if (state->right_chunk >= state->right_chunks.chunks.size()) {
		return;
	}
	// now that we have fully materialized the right child
	// we have to perform the nested loop join

	do {
		// first check if we have to move to the next child on the right isde
		assert(state->right_chunk < state->right_chunks.chunks.size());
		if (state->right_tuple >=
		    state->right_chunks.chunks[state->right_chunk]->size()) {
			// we exhausted the chunk on the right
			state->right_chunk++;
			if (state->right_chunk >= state->right_chunks.chunks.size()) {
				// we exhausted all right chunks!
				// move to the next left chunk
				children[0]->GetChunk(context, state->child_chunk,
				                      state->child_state.get());
				if (state->child_chunk.size() == 0) {
					return;
				}
				state->child_chunk.Flatten();

				state->right_chunk = 0;

				// resolve the left join condition for the current chunk
				state->left_join_condition.Reset();
				ExpressionExecutor executor(state->child_chunk, context);
				executor.Execute(
				    state->left_join_condition,
				    [&](size_t i) { return conditions[i].left.get(); },
				    conditions.size());
			}
			// move to the start of this chunk
			state->left_tuple = 0;
			state->right_tuple = 0;
		}

		auto &left_chunk = state->child_chunk;
		auto &right_chunk = *state->right_chunks.chunks[state->right_chunk];
		auto &right_data = *state->right_data.chunks[state->right_chunk];

		// sanity check, this went wrong before
		left_chunk.Verify();
		right_chunk.Verify();
		right_data.Verify();

		// now perform the join

		// first we create a selection vector from the first condition
		// here we do a nested loop join
		sel_t lvector[STANDARD_VECTOR_SIZE], rvector[STANDARD_VECTOR_SIZE];
		size_t match_count = 0;

		match_count = nested_loop_join(conditions[0].comparison,
		                               state->left_join_condition.data[0],
		                               right_chunk.data[0], state->left_tuple,
		                               state->right_tuple, lvector, rvector);

		// now resolve the rest of the conditions
		for (size_t i = 1; i < conditions.size(); i++) {
			// for all the matches we have obtained
			// we perform the comparisons
			if (match_count == 0) {
				break;
			}
			// get the vectors to compare
			Vector &l = state->left_join_condition.data[i];
			Vector &r = right_chunk.data[i];

			// now perform the actual comparisons
			match_count = nested_loop_comparison(conditions[i].comparison, l, r,
			                                     lvector, rvector, match_count);
		}

		// we have finished resolving the join conditions
		if (match_count == 0) {
			// if there are no results, move on
			continue;
		}
		// we have matching tuples!
		// construct the result
		// create a reference to the chunk on the left side
		for (size_t i = 0; i < state->child_chunk.column_count; i++) {
			chunk.data[i].Reference(state->child_chunk.data[i]);
			chunk.data[i].count = match_count;
			chunk.data[i].sel_vector = lvector;
			chunk.data[i].Flatten();
		}
		// now create a reference to the chunk on the right side
		for (size_t i = 0; i < right_data.column_count; i++) {
			size_t chunk_entry = state->child_chunk.column_count + i;
			chunk.data[chunk_entry].Reference(right_data.data[i]);
			chunk.data[chunk_entry].count = match_count;
			chunk.data[chunk_entry].sel_vector = rvector;
			chunk.data[chunk_entry].Flatten();
		}
		chunk.sel_vector = nullptr;
	} while (chunk.size() == 0);
}

unique_ptr<PhysicalOperatorState> PhysicalNestedLoopJoinInner::GetOperatorState(
    ExpressionExecutor *parent_executor) {
	return make_unique<PhysicalNestedLoopJoinInnerOperatorState>(
	    children[0].get(), children[1].get(), parent_executor);
}

} // namespace duckdb
