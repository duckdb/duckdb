#include "execution/operator/join/physical_piecewise_merge_join.hpp"

#include "common/operator/comparison_operators.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"

using namespace duckdb;
using namespace operators;
using namespace std;

struct MergeInfo {
	Vector &v;
	size_t count;
	sel_t *sel_vector;
	size_t &pos;
	sel_t result[STANDARD_VECTOR_SIZE];

	MergeInfo(Vector &v, size_t count, sel_t *sel_vector, size_t &pos)
	    : v(v), count(count), sel_vector(sel_vector), pos(pos) {
	}
};

struct MergeJoinEquality {
	template <class T> static size_t Operation(MergeInfo &l, MergeInfo &r) {
		if (l.pos >= l.count) {
			return 0;
		}
		assert(l.sel_vector && r.sel_vector);
		auto ldata = (T *)l.v.data;
		auto rdata = (T *)r.v.data;
		size_t result_count = 0;
		while (true) {
			if (r.pos == r.count || LessThan::Operation(ldata[l.sel_vector[l.pos]], rdata[r.sel_vector[r.pos]])) {
				// left side smaller: move left pointer forward
				l.pos++;
				if (l.pos >= l.count) {
					// left side exhausted
					break;
				}
				// we might need to go back on the right-side after going
				// forward on the left side because the new tuple might have
				// matches with the right side
				while (r.pos > 0 && Equals::Operation(ldata[l.sel_vector[l.pos]], rdata[r.sel_vector[r.pos - 1]])) {
					r.pos--;
				}
			} else if (GreaterThan::Operation(ldata[l.sel_vector[l.pos]], rdata[r.sel_vector[r.pos]])) {
				// right side smaller: move right pointer forward
				r.pos++;
			} else {
				// tuples match
				// output tuple
				l.result[result_count] = l.sel_vector[l.pos];
				r.result[result_count] = r.sel_vector[r.pos];
				result_count++;
				// move right side forward
				r.pos++;
				if (result_count == STANDARD_VECTOR_SIZE) {
					// out of space!
					break;
				}
			}
		}
		return result_count;
	}
};

struct MergeJoinLessThan {
	template <class T> static size_t Operation(MergeInfo &l, MergeInfo &r) {
		if (r.pos >= r.count) {
			return 0;
		}
		assert(l.sel_vector && r.sel_vector);
		auto ldata = (T *)l.v.data;
		auto rdata = (T *)r.v.data;
		size_t result_count = 0;
		while (true) {
			if (l.pos < l.count && LessThan::Operation(ldata[l.sel_vector[l.pos]], rdata[r.sel_vector[r.pos]])) {
				// left side smaller: found match
				l.result[result_count] = l.sel_vector[l.pos];
				r.result[result_count] = r.sel_vector[r.pos];
				result_count++;
				// move left side forward
				l.pos++;
				if (result_count == STANDARD_VECTOR_SIZE) {
					// out of space!
					break;
				}
			} else {
				// right side smaller or equal, or left side exhausted: move
				// right pointer forward reset left side to start
				l.pos = 0;
				r.pos++;
				if (r.pos == r.count) {
					break;
				}
			}
		}
		return result_count;
	}
};

struct MergeJoinLessThanEquals {
	template <class T> static size_t Operation(MergeInfo &l, MergeInfo &r) {
		if (r.pos >= r.count) {
			return 0;
		}
		assert(l.sel_vector && r.sel_vector);
		auto ldata = (T *)l.v.data;
		auto rdata = (T *)r.v.data;
		size_t result_count = 0;
		while (true) {
			if (l.pos < l.count && LessThanEquals::Operation(ldata[l.sel_vector[l.pos]], rdata[r.sel_vector[r.pos]])) {
				// left side smaller: found match
				l.result[result_count] = l.sel_vector[l.pos];
				r.result[result_count] = r.sel_vector[r.pos];
				result_count++;
				// move left side forward
				l.pos++;
				if (result_count == STANDARD_VECTOR_SIZE) {
					// out of space!
					break;
				}
			} else {
				// right side smaller or equal, or left side exhausted: move
				// right pointer forward reset left side to start
				l.pos = 0;
				r.pos++;
				if (r.pos == r.count) {
					break;
				}
			}
		}
		return result_count;
	}
};

template <class MJ> static size_t merge_join(MergeInfo &l, MergeInfo &r) {
	assert(l.v.type == r.v.type);
	if (l.count == 0 || r.count == 0) {
		return 0;
	}
	switch (l.v.type) {
	case TypeId::TINYINT:
		return MJ::template Operation<int8_t>(l, r);
	case TypeId::SMALLINT:
		return MJ::template Operation<int16_t>(l, r);
	case TypeId::DATE:
	case TypeId::INTEGER:
		return MJ::template Operation<int32_t>(l, r);
	case TypeId::TIMESTAMP:
	case TypeId::BIGINT:
		return MJ::template Operation<int64_t>(l, r);
	case TypeId::DECIMAL:
		return MJ::template Operation<double>(l, r);
	case TypeId::VARCHAR:
		return MJ::template Operation<const char *>(l, r);
	default:
		throw NotImplementedException("Type not for merge join implemented!");
	}
}

static size_t MergeJoin(MergeInfo &l, MergeInfo &r, ExpressionType comparison_type) {
	if (comparison_type == ExpressionType::COMPARE_EQUAL) {
		return merge_join<MergeJoinEquality>(l, r);
	} else if (comparison_type == ExpressionType::COMPARE_LESSTHAN) {
		return merge_join<MergeJoinLessThan>(l, r);
	} else if (comparison_type == ExpressionType::COMPARE_LESSTHANOREQUALTO) {
		return merge_join<MergeJoinLessThanEquals>(l, r);
	} else if (comparison_type == ExpressionType::COMPARE_GREATERTHAN ||
	           comparison_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
		// simply flip the comparison type
		return MergeJoin(r, l, LogicalJoin::FlipComparisionExpression(comparison_type));
	} else {
		throw Exception("Unimplemented comparison type for merge join!");
	}
}

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
	sel_t *result_vector;
	order.count = Vector::NotNullSelVector(vector, order.order, result_vector);
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
		size_t result_count = MergeJoin(left, right, conditions[0].comparison);
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
