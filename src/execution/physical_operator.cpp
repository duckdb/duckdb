
#include "execution/physical_operator.hpp"

using namespace duckdb;
using namespace std;

string PhysicalOperator::ToString() const {
	string result = PhysicalOperatorToString(type);
	if (children.size() > 0) {
		result += " ( ";
		for (auto &child : children) {
			result += child->ToString();
		}
		result += " )";
	}
	return result;
}

PhysicalOperatorState::PhysicalOperatorState(
    PhysicalOperator *child, ExpressionExecutor *parent_executor)
    : finished(false), parent(parent_executor) {
	if (child) {
		child->InitializeChunk(child_chunk);
		child_state = child->GetOperatorState(parent_executor);
	}
}
