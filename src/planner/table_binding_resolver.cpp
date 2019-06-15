#include "planner/table_binding_resolver.hpp"

#include "catalog/catalog_entry/table_catalog_entry.hpp"
#include "catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "planner/operator/list.hpp"

using namespace duckdb;
using namespace std;

TableBindingResolver::TableBindingResolver(bool recurse_into_subqueries, bool visit_expressions)
    : recurse_into_subqueries(recurse_into_subqueries), visit_expressions(visit_expressions) {
}

void TableBindingResolver::PushBinding(BoundTable binding) {
	binding.column_offset =
	    bound_tables.size() == 0 ? 0 : bound_tables.back().column_offset + bound_tables.back().column_count;
	bound_tables.push_back(binding);
}

void TableBindingResolver::AppendTables(vector<BoundTable> &right_tables) {
	index_t offset =
	    bound_tables.size() == 0 ? 0 : bound_tables.back().column_offset + bound_tables.back().column_count;
	for (auto table : right_tables) {
		table.column_offset += offset;
		bound_tables.push_back(table);
	}
}

void TableBindingResolver::VisitOperator(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::AGGREGATE_AND_GROUP_BY:
		Visit((LogicalAggregate &)op);
		break;
	case LogicalOperatorType::WINDOW:
		Visit((LogicalWindow &)op);
		break;
	case LogicalOperatorType::EMPTY_RESULT:
		Visit((LogicalEmptyResult &)op);
		break;
	case LogicalOperatorType::PROJECTION:
		Visit((LogicalProjection &)op);
		break;
	case LogicalOperatorType::CHUNK_GET:
		Visit((LogicalChunkGet &)op);
		break;
	case LogicalOperatorType::EXPRESSION_GET:
		Visit((LogicalExpressionGet &)op);
		break;
	case LogicalOperatorType::DELIM_GET:
		Visit((LogicalDelimGet &)op);
		break;
	case LogicalOperatorType::GET:
		Visit((LogicalGet &)op);
		break;
	case LogicalOperatorType::SUBQUERY:
		Visit((LogicalSubquery &)op);
		break;
	case LogicalOperatorType::TABLE_FUNCTION:
		Visit((LogicalTableFunction &)op);
		break;
	case LogicalOperatorType::ANY_JOIN:
		Visit((LogicalAnyJoin &)op);
		break;
	case LogicalOperatorType::DELIM_JOIN:
	case LogicalOperatorType::COMPARISON_JOIN:
		Visit((LogicalComparisonJoin &)op);
		break;
	case LogicalOperatorType::CROSS_PRODUCT:
		Visit((LogicalCrossProduct &)op);
		break;
	case LogicalOperatorType::EXCEPT:
	case LogicalOperatorType::INTERSECT:
	case LogicalOperatorType::UNION:
		Visit((LogicalSetOperation &)op);
		break;
	case LogicalOperatorType::CREATE_INDEX:
		Visit((LogicalCreateIndex &)op);
		break;
	case LogicalOperatorType::INDEX_SCAN:
		Visit((LogicalIndexScan &)op);
		break;
	default:
		// for the operators we do not handle explicitly, we just visit the children
		LogicalOperatorVisitor::VisitOperator(op);
		break;
	}
}

void TableBindingResolver::VisitExpression(unique_ptr<Expression> *expression) {
	if (!visit_expressions) {
		return;
	}
	LogicalOperatorVisitor::VisitExpression(expression);
}

void TableBindingResolver::Visit(LogicalEmptyResult &op) {
	// empty result; bind the tables that would be bound here if the subtree was not pruned
	bound_tables = op.bound_tables;
}

void TableBindingResolver::Visit(LogicalCreateIndex &op) {
	// add the table to the column binding resolver
	// since we only have one table in the CREATE INDEX statement there is no
	// offset
	BoundTable binding;
	binding.table_index = 0;
	binding.column_count = op.table.columns.size();
	binding.column_offset = 0;
	bound_tables.push_back(binding);

	LogicalOperatorVisitor::VisitOperatorExpressions(op);
}

void TableBindingResolver::BindTablesBinaryOp(LogicalOperator &op, bool append_right) {
	assert(op.children.size() == 2);
	// resolve the column indices of the left side
	VisitOperator(*op.children[0]);
	// store the added tables
	auto left_tables = bound_tables;
	bound_tables.clear();

	// now resolve the column indices of the right side
	VisitOperator(*op.children[1]);
	auto right_tables = bound_tables;

	// now merge the two together
	bound_tables = left_tables;
	if (append_right) {
		AppendTables(right_tables);
	}
}

void TableBindingResolver::RecurseIntoSubquery(LogicalOperator &op) {
	if (!recurse_into_subqueries) {
		return;
	}
	assert(op.children.size() == 1);
	// we clear the bound tables prior to visiting this operator
	auto old_tables = bound_tables;
	bound_tables.clear();
	LogicalOperatorVisitor::VisitOperator(op);
	bound_tables = old_tables;
}

void TableBindingResolver::Visit(LogicalAggregate &op) {
	RecurseIntoSubquery(op);

	BoundTable group_binding;
	group_binding.table_index = op.group_index;
	group_binding.column_count = op.groups.size();
	PushBinding(group_binding);

	BoundTable aggregate_binding;
	aggregate_binding.table_index = op.aggregate_index;
	aggregate_binding.column_count = op.expressions.size();
	PushBinding(aggregate_binding);
}

void TableBindingResolver::Visit(LogicalSubquery &op) {
	RecurseIntoSubquery(op);

	BoundTable binding;
	binding.table_index = op.table_index;
	binding.column_count = op.column_count;
	PushBinding(binding);
}

void TableBindingResolver::Visit(LogicalProjection &op) {
	RecurseIntoSubquery(op);

	BoundTable binding;
	binding.table_index = op.table_index;
	binding.column_count = op.expressions.size();
	PushBinding(binding);
}

void TableBindingResolver::Visit(LogicalWindow &op) {
	// the LogicalWindow pushes all underlying expressions through
	// hence we can visit it normally
	LogicalOperatorVisitor::VisitOperator(op);

	BoundTable binding;
	binding.table_index = op.window_index;
	binding.column_count = op.expressions.size();
	PushBinding(binding);
}

void TableBindingResolver::Visit(LogicalCrossProduct &op) {
	BindTablesBinaryOp(op, true);
}

void TableBindingResolver::Visit(LogicalSetOperation &op) {
	assert(op.children.size() == 2);
	if (recurse_into_subqueries) {
		// have to visit the children of the set operator
		auto old_tables = bound_tables;
		bound_tables.clear();
		LogicalOperatorVisitor::VisitOperator(*op.children[0]);
		bound_tables.clear();
		LogicalOperatorVisitor::VisitOperator(*op.children[1]);
		bound_tables = old_tables;
	}
	// push the binding referring to this setop
	BoundTable binding;
	binding.table_index = op.table_index;
	binding.column_count = op.column_count;
	PushBinding(binding);
}

void TableBindingResolver::Visit(LogicalExpressionGet &op) {
	RecurseIntoSubquery(op);

	BoundTable binding;
	binding.table_index = op.table_index;
	binding.column_count = op.expr_types.size();
	PushBinding(binding);
}

void TableBindingResolver::Visit(LogicalChunkGet &op) {
	BoundTable binding;
	binding.table_index = op.table_index;
	binding.column_count = op.chunk_types.size();
	PushBinding(binding);
}

void TableBindingResolver::Visit(LogicalDelimGet &op) {
	BoundTable binding;
	binding.table_index = op.table_index;
	binding.column_count = op.chunk_types.size();
	PushBinding(binding);
}

void TableBindingResolver::Visit(LogicalGet &op) {
	BoundTable binding;
	if (!op.table) {
		// DUMMY get
		// create a dummy table with a single column
		binding.table_index = INVALID_INDEX;
		binding.column_count = 1;
	} else {
		binding.table_index = op.table_index;
		binding.column_count = op.column_ids.size();
	}
	PushBinding(binding);
}

void TableBindingResolver::Visit(LogicalIndexScan &op) {
	BoundTable binding;
	//	if (!op.table.) {
	//		binding.table_index = (size_t)-1;
	//		binding.column_count = 1;
	//	} else {
	binding.table_index = op.table_index;
	binding.column_count = op.column_ids.size();
	//	}
	PushBinding(binding);
}

void TableBindingResolver::Visit(LogicalTableFunction &op) {
	LogicalOperatorVisitor::VisitOperatorExpressions(op);

	BoundTable binding;
	binding.table_index = op.table_index;
	binding.column_count = op.function->return_values.size();
	PushBinding(binding);
}

void TableBindingResolver::Visit(LogicalAnyJoin &op) {
	// visit the LHS
	VisitOperator(*op.children[0]);
	// store the added tables
	auto left_tables = bound_tables;
	bound_tables.clear();

	// visit the RHS
	VisitOperator(*op.children[1]);
	auto right_tables = bound_tables;
	// concatenate the tables
	bound_tables = left_tables;
	AppendTables(right_tables);
	// now visit the join condition
	VisitExpression(&op.condition);
	if (op.type == JoinType::ANTI || op.type == JoinType::SEMI) {
		// (not supported yet for arbitrary expressions)
		assert(0);
		// for semi/anti joins the result is just the left side
		bound_tables = left_tables;
	}
}

void TableBindingResolver::Visit(LogicalComparisonJoin &op) {
	// resolve the column indices of the left side
	VisitOperator(*op.children[0]);
	for (auto &cond : op.conditions) {
		VisitExpression(&cond.left);
	}
	if (op.GetOperatorType() == LogicalOperatorType::DELIM_JOIN) {
		// visit the duplicate eliminated columns on the LHS, if any
		auto &delim_join = (LogicalDelimJoin &)op;
		for (auto &expr : delim_join.duplicate_eliminated_columns) {
			VisitExpression(&expr);
		}
	}
	// store the added tables
	auto left_tables = bound_tables;
	bound_tables.clear();

	// now resolve the column indices of the right side
	VisitOperator(*op.children[1]);
	for (auto &cond : op.conditions) {
		VisitExpression(&cond.right);
	}
	auto right_tables = bound_tables;

	bound_tables = left_tables;
	if (op.type == JoinType::ANTI || op.type == JoinType::SEMI) {
		// for semi/anti joins the result is just the left side
		return;
	}
	if (op.type == JoinType::MARK) {
		// for MARK join the result is the LEFT side, plus a table that has a single column (the MARK column)
		assert(op.children[1]->type == LogicalOperatorType::SUBQUERY);
		// the immediate RIGHT side should be a SUBQUERY
		auto &subquery = (LogicalSubquery &)*op.children[1];
		BoundTable binding;
		binding.table_index = subquery.table_index;
		binding.column_count = 1;
		PushBinding(binding);
		return;
	}

	// for other joins the two results are combined
	AppendTables(right_tables);
}
