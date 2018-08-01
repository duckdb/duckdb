
#include "planner/logical_plan_generator.hpp"

#include "parser/expression/expression_list.hpp"
#include "parser/statement/insert_statement.hpp"

#include "planner/operator/logical_aggregate.hpp"
#include "planner/operator/logical_distinct.hpp"
#include "planner/operator/logical_filter.hpp"
#include "planner/operator/logical_get.hpp"
#include "planner/operator/logical_insert.hpp"
#include "planner/operator/logical_limit.hpp"
#include "planner/operator/logical_order.hpp"
#include "planner/operator/logical_projection.hpp"

using namespace duckdb;
using namespace std;

void LogicalPlanGenerator::Visit(SelectStatement &statement) {
	for (auto &expr : statement.select_list) {
		expr->Accept(this);
	}

	if (statement.from_table) {
		// SELECT with FROM
		statement.from_table->Accept(this);
	} else {
		// SELECT without FROM, add empty GET
		root = make_unique<LogicalGet>();
	}

	if (statement.where_clause) {
		statement.where_clause->Accept(this);

		auto filter = make_unique<LogicalFilter>(move(statement.where_clause));
		filter->children.push_back(move(root));
		root = move(filter);
	}

	if (statement.HasAggregation()) {
		auto aggregate =
		    make_unique<LogicalAggregate>(move(statement.select_list));
		if (statement.HasGroup()) {
			// have to add group by columns
			aggregate->groups = move(statement.groupby.groups);
		}
		aggregate->children.push_back(move(root));
		root = move(aggregate);

		if (statement.HasHaving()) {
			statement.groupby.having->Accept(this);

			auto having =
			    make_unique<LogicalFilter>(move(statement.groupby.having));
			having->children.push_back(move(root));
			root = move(having);
		}
	} else {
		auto projection =
		    make_unique<LogicalProjection>(move(statement.select_list));
		projection->children.push_back(move(root));
		root = move(projection);
	}

	if (statement.select_distinct) {
		auto distinct = make_unique<LogicalDistinct>();
		distinct->children.push_back(move(root));
		root = move(distinct);
	}
	if (statement.HasOrder()) {
		auto order = make_unique<LogicalOrder>(move(statement.orderby));
		order->children.push_back(move(root));
		root = move(order);
	}
	if (statement.HasLimit()) {
		auto limit = make_unique<LogicalLimit>(statement.limit.limit,
		                                       statement.limit.offset);
		limit->children.push_back(move(root));
		root = move(limit);
	}
}

static void cast_children_to_equal_types(AbstractExpression &expr) {
	if (expr.children.size() == 2) {
		TypeId left_type = expr.children[0]->return_type;
		TypeId right_type = expr.children[1]->return_type;
		if (left_type != right_type) {
			// types don't match
			// we have to add a cast
			if (left_type < right_type) {
				// add cast on left hand side
				auto cast = make_unique<CastExpression>(right_type,
				                                        move(expr.children[0]));
				expr.children[0] = move(cast);
			} else {
				// add cast on right hand side
				auto cast = make_unique<CastExpression>(left_type,
				                                        move(expr.children[1]));
				expr.children[1] = move(cast);
			}
		}
	}
}

void LogicalPlanGenerator::Visit(BaseTableRefExpression &expr) {
	auto table = catalog.GetTable(expr.schema_name, expr.table_name);
	auto get_table = make_unique<LogicalGet>(
	    table, expr.alias.empty() ? expr.table_name : expr.alias);
	if (root)
		get_table->children.push_back(move(root));
	root = move(get_table);
}

void LogicalPlanGenerator::Visit(ComparisonExpression &expr) {
	SQLNodeVisitor::Visit(expr);
	cast_children_to_equal_types(expr);
}

void LogicalPlanGenerator::Visit(ConjunctionExpression &expr) {
	SQLNodeVisitor::Visit(expr);
	cast_children_to_equal_types(expr);
}

void LogicalPlanGenerator::Visit(JoinExpression &expr) {
	throw NotImplementedException("Joins not implemented yet!");
}

void LogicalPlanGenerator::Visit(OperatorExpression &expr) {
	SQLNodeVisitor::Visit(expr);
	cast_children_to_equal_types(expr);
}

void LogicalPlanGenerator::Visit(SubqueryExpression &expr) {
	throw NotImplementedException("Subquery not implemented yet!");
}

void LogicalPlanGenerator::Visit(InsertStatement &statement) {
	auto table = catalog.GetTable(statement.schema, statement.table);
	auto insert = make_unique<LogicalInsert>(table, move(statement.values));
	root = move(insert);
}
