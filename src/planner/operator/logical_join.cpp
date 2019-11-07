#include "duckdb/planner/operator/logical_join.hpp"

#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/table_binding_resolver.hpp"

using namespace duckdb;
using namespace std;

LogicalJoin::LogicalJoin(JoinType type, LogicalOperatorType logical_type) : LogicalOperator(logical_type), type(type) {
}

void LogicalJoin::ResolveTypes() {
	types.insert(types.end(), children[0]->types.begin(), children[0]->types.end());
	if (type == JoinType::SEMI || type == JoinType::ANTI) {
		// for SEMI and ANTI join we only project the left hand side
		return;
	}
	if (type == JoinType::MARK) {
		// for MARK join we project the left hand side, plus a BOOLEAN column indicating the MARK
		types.push_back(TypeId::BOOLEAN);
		return;
	}
	// for any other join we project both sides
	types.insert(types.end(), children[1]->types.begin(), children[1]->types.end());
}

void LogicalJoin::GetTableReferences(LogicalOperator &op, unordered_set<index_t> &bindings) {
	TableBindingResolver resolver;
	resolver.VisitOperator(op);
	for (auto &table : resolver.bound_tables) {
		bindings.insert(table.table_index);
	}
}

void LogicalJoin::GetExpressionBindings(Expression &expr, unordered_set<index_t> &bindings) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = (BoundColumnRefExpression &)expr;
		assert(colref.depth == 0);
		bindings.insert(colref.binding.table_index);
	}
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) { GetExpressionBindings(child, bindings); });
}
