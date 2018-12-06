#include "execution/operator/aggregate/physical_window.hpp"

#include "execution/expression_executor.hpp"
#include "parser/expression/aggregate_expression.hpp"
#include "parser/expression/constant_expression.hpp"

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
	throw Exception("Eek");
}



PhysicalWindowOperatorState::PhysicalWindowOperatorState(PhysicalWindow *parent, PhysicalOperator *child,
                                                               ExpressionExecutor *parent_executor)
    : PhysicalOperatorState(child, parent_executor) {
	throw Exception("eek");

}
