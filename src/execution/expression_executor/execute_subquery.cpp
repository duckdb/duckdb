#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"
#include "parser/expression/subquery_expression.hpp"

using namespace duckdb;
using namespace std;

void ExpressionExecutor::Visit(SubqueryExpression &expr) {
	auto &plan = expr.plan;
	if (!plan) {
		throw Exception("Failed to generate query plan for subquery");
	}
	DataChunk *old_chunk = chunk;
	DataChunk row_chunk;
	if (old_chunk) {
		chunk = &row_chunk;
		auto types = old_chunk->GetTypes();
		row_chunk.Initialize(types, true);
		for (size_t c = 0; c < old_chunk->column_count; c++) {
			row_chunk.data[c].count = 1;
		}
	}
	vector.Initialize(expr.return_type);
	vector.count = old_chunk ? old_chunk->size() : 1;
	vector.sel_vector = old_chunk ? old_chunk->sel_vector : nullptr;

	for (size_t r = 0; r < vector.count; r++) {
		if (old_chunk) {
			for (size_t c = 0; c < row_chunk.column_count; c++) {
				row_chunk.data[c].SetValue(0, old_chunk->data[c].GetValue(r));
			}
		}
		auto state = plan->GetOperatorState(this);
		DataChunk s_chunk;
		plan->InitializeChunk(s_chunk);
		plan->GetChunk(context, s_chunk, state.get());

		switch (expr.subquery_type) {
		case SubqueryType::DEFAULT:
			if (s_chunk.size() == 0) {
				vector.SetValue(r, Value());
			} else {
				assert(s_chunk.column_count > 0);
				vector.SetValue(r, s_chunk.data[0].GetValue(0));
			}
			break;

		case SubqueryType::IN: // in case is handled separately above
		case SubqueryType::EXISTS:
			vector.SetValue(r, Value::BOOLEAN(s_chunk.size() != 0));
			break;
		default:
			throw NotImplementedException("Subquery type not implemented");
		}
	}
	chunk = old_chunk;
	Verify(expr);
}
