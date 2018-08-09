
#include "planner/logical_visitor.hpp"

#include "planner/operator/logical_aggregate.hpp"
#include "planner/operator/logical_distinct.hpp"
#include "planner/operator/logical_filter.hpp"
#include "planner/operator/logical_get.hpp"
#include "planner/operator/logical_insert.hpp"
#include "planner/operator/logical_copy.hpp"
#include "planner/operator/logical_limit.hpp"
#include "planner/operator/logical_order.hpp"
#include "planner/operator/logical_projection.hpp"

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
void LogicalOperatorVisitor::Visit(LogicalCopy &op) {
	op.AcceptChildren(this);
}
