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
			index_t new_count = Select(*expr.children[i], state->child_states[i].get(), result);
			if (new_count == 0) {
				return 0;
			}
			if (new_count != current_count) {
				SetChunkSelectionVector(*chunk, result, new_count);
				current_count = new_count;
			}
		}
		// restore the initial selection vector and count
		SetChunkSelectionVector(*chunk, initial_sel, initial_count);
		return current_count;
	} else {
		return DefaultSelect(expr, state, result);
	}
}
