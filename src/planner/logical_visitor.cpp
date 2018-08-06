
#include "planner/logical_visitor.hpp"

#include "planner/operator/logical_list.hpp"

using namespace duckdb;
using namespace std;

void LogicalOperatorVisitor::Visit(LogicalAggregate &op) {
	op.AcceptChildren(this);
}
void LogicalOperatorVisitor::Visit(LogicalDistinct &op) {
	op.AcceptChildren(this);
}
void LogicalOperatorVisitor::Visit(LogicalFilter &op) {
	op.AcceptChildren(this);
}
void LogicalOperatorVisitor::Visit(LogicalGet &op) { op.AcceptChildren(this); }
void LogicalOperatorVisitor::Visit(LogicalLimit &op) {
	op.AcceptChildren(this);
}
void LogicalOperatorVisitor::Visit(LogicalOrder &op) {
	op.AcceptChildren(this);
}
void LogicalOperatorVisitor::Visit(LogicalProjection &op) {
	op.AcceptChildren(this);
}
void LogicalOperatorVisitor::Visit(LogicalInsert &op) {
	op.AcceptChildren(this);
}
