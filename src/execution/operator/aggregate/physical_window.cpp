#include "execution/operator/aggregate/physical_window.hpp"

#include "execution/expression_executor.hpp"
#include "parser/expression/aggregate_expression.hpp"
#include "parser/expression/constant_expression.hpp"

#include "common/vector_operations/vector_operations.hpp"


using namespace duckdb;
using namespace std;

PhysicalWindow::PhysicalWindow(LogicalOperator &op, vector<unique_ptr<Expression>> select_list,
                                     PhysicalOperatorType type)
    : PhysicalOperator(type, op.types), select_list(std::move(select_list)) {
	Initialize();
}

void PhysicalWindow::Initialize() {
}

void PhysicalWindow::_GetChunk(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalWindowOperatorState *>(state_);
	do {
		children[0]->GetChunk(context, chunk, state->child_state.get());
		if (chunk.size() == 0) {
			return;
		}

		assert(chunk.column_count == select_list.size());
		// set the vectors for the windows to NULL
		for (size_t expr_idx = 0; expr_idx < select_list.size(); expr_idx++) {
			if (select_list[expr_idx]->GetExpressionClass() == ExpressionClass::WINDOW) {
				VectorOperations::Set(chunk.data[expr_idx], Value());
			}
		}
	} while (chunk.size() == 0);
}

PhysicalWindowOperatorState::PhysicalWindowOperatorState(PhysicalWindow *parent, PhysicalOperator *child,
                                                               ExpressionExecutor *parent_executor)
    : PhysicalOperatorState(child, parent_executor) {
	throw Exception("eek");

}
