#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(BoundConjunctionExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_unique<ExpressionState>(expr, root);
	vector<Expression *> children;
	for (auto &child : expr.children) {
		children.push_back(child.get());
	}
	result->AddIntermediates(children);
	return result;
}

void ExpressionExecutor::Execute(BoundConjunctionExpression &expr, ExpressionState *state, Vector &result) {
	// execute the children
	for (index_t i = 0; i < expr.children.size(); i++) {
		Execute(*expr.children[i], state->child_states[i].get(), state->arguments.data[i]);
		if (i == 0) {
			// move the result
			result.Reference(state->arguments.data[i]);
		} else {
			Vector intermediate(TypeId::BOOLEAN, true, false);
			// AND/OR together
			switch (expr.type) {
			case ExpressionType::CONJUNCTION_AND:
				VectorOperations::And(state->arguments.data[i], result, intermediate);
				break;
			case ExpressionType::CONJUNCTION_OR:
				VectorOperations::Or(state->arguments.data[i], result, intermediate);
				break;
			default:
				throw NotImplementedException("Unknown conjunction type!");
			}
			intermediate.Move(result);
		}
	}
}

static void SetChunkSelectionVector(DataChunk &chunk, sel_t *sel_vector, index_t count) {
	chunk.sel_vector = sel_vector;
	for (index_t col_idx = 0; col_idx < chunk.column_count; col_idx++) {
		chunk.data[col_idx].count = count;
		chunk.data[col_idx].sel_vector = sel_vector;
	}
}

static void MergeSelectionVectorIntoResult(sel_t *result, index_t &result_count, sel_t *sel, index_t count) {
	assert(count > 0);
	if (result_count == 0) {
		// nothing to merge
		memcpy(result, sel, count * sizeof(sel_t));
		result_count = count;
		return;
	}

	sel_t temp_result[STANDARD_VECTOR_SIZE];
	index_t res_idx = 0, sel_idx = 0;
	index_t temp_count = 0;
	while (true) {
		// the two sets should be disjunct
		assert(result[res_idx] != sel[sel_idx]);
		if (result[res_idx] < sel[sel_idx]) {
			temp_result[temp_count++] = result[res_idx];
			res_idx++;
			if (res_idx >= result_count) {
				break;
			}
		} else {
			assert(result[res_idx] > sel[sel_idx]);
			temp_result[temp_count++] = sel[sel_idx];
			sel_idx++;
			if (sel_idx >= count) {
				break;
			}
		}
	}
	// append remaining entries
	if (sel_idx < count) {
		// first copy the temp_result to the result
		memcpy(result, temp_result, temp_count * sizeof(sel_t));
		// now copy the remaining entries in the selection vector after the initial result
		memcpy(result + temp_count, sel + sel_idx, (count - sel_idx) * sizeof(sel_t));
		result_count = temp_count + count - sel_idx;
	} else {
		// first copy the remainder of the result into the temp_result vector
		memcpy(temp_result + temp_count, result + res_idx, (result_count - res_idx) * sizeof(sel_t));
		result_count = temp_count + (result_count - res_idx);
		// now copy the temp_result back into the main result vector
		memcpy(result, temp_result, result_count * sizeof(sel_t));
	}
}

index_t ExpressionExecutor::Select(BoundConjunctionExpression &expr, ExpressionState *state, sel_t result[]) {
	if (!chunk) {
		return DefaultSelect(expr, state, result);
	}
	if (expr.type == ExpressionType::CONJUNCTION_AND) {
		// store the initial selection vector and count
		auto initial_sel = chunk->sel_vector;
		index_t initial_count = chunk->size();
		index_t current_count = chunk->size();

		for (index_t i = 0; i < expr.children.size(); i++) {
			// first resolve the current expression
			index_t new_count = Select(*expr.children[i], state->child_states[i].get(), result);
			if (new_count == 0) {
				current_count = 0;
				break;
			}
			if (new_count != current_count) {
				// disqualify all non-qualifying tuples by updating the selection vector
				SetChunkSelectionVector(*chunk, result, new_count);
				current_count = new_count;
			}
		}
		// restore the initial selection vector and count
		SetChunkSelectionVector(*chunk, initial_sel, initial_count);
		return current_count;
	} else {
		sel_t *initial_sel = chunk->sel_vector;
		index_t initial_count = chunk->size();
		index_t current_count = chunk->size();
		sel_t *current_sel = initial_sel;

		sel_t intermediate_result[STANDARD_VECTOR_SIZE];
		sel_t expression_result[STANDARD_VECTOR_SIZE];
		sel_t remaining[STANDARD_VECTOR_SIZE];
		index_t result_count = 0;
		index_t remaining_count = 0;
		sel_t *result_vector = initial_sel == result ? intermediate_result : result;
		for (index_t expr_idx = 0; expr_idx < expr.children.size(); expr_idx++) {
			// first resolve the current expression
			index_t new_count =
			    Select(*expr.children[expr_idx], state->child_states[expr_idx].get(), expression_result);
			if (new_count == 0) {
				// no new qualifying entries: continue
				continue;
			}
			if (new_count == current_count) {
				// all remaining entries qualified! add them to the result
				if (!current_sel) {
					// first iteration already passes all tuples, no need to set up selection vector
					assert(current_count == initial_count);
					result_count = initial_count;
					break;
				}
				MergeSelectionVectorIntoResult(result_vector, result_count, current_sel, current_count);
				break;
			}
			// first merge the current results back into the result vector
			MergeSelectionVectorIntoResult(result_vector, result_count, expression_result, new_count);
			if (expr_idx + 1 == expr.children.size()) {
				// this is the last child: we don't need to construct the remaining tuples
				break;
			}
			// now we only need to continue executing tuples that were not qualified
			// we figure this out by performing a merge of the remaining tuples and the resulting selection vector
			index_t new_idx = 0;
			remaining_count = 0;
			for (index_t i = 0; i < current_count; i++) {
				auto entry = current_sel ? current_sel[i] : i;
				if (new_idx >= new_count || expression_result[new_idx] != entry) {
					remaining[remaining_count++] = entry;
				} else {
					new_idx++;
				}
			}
			current_sel = remaining;
			current_count = remaining_count;
			SetChunkSelectionVector(*chunk, remaining, remaining_count);
		}
		SetChunkSelectionVector(*chunk, initial_sel, initial_count);
		if (result_vector != result && result_count > 0) {
			memcpy(result, result_vector, result_count * sizeof(sel_t));
		}
		return result_count;
	}
}
