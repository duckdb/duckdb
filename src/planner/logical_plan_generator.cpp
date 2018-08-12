
#include "planner/logical_plan_generator.hpp"

#include "parser/expression/expression_list.hpp"
#include "parser/statement/insert_statement.hpp"
#include "parser/statement/copy_statement.hpp"
#include "parser/tableref/tableref_list.hpp"

#include "planner/operator/logical_aggregate.hpp"
#include "planner/operator/logical_distinct.hpp"
#include "planner/operator/logical_filter.hpp"
#include "planner/operator/logical_get.hpp"
#include "planner/operator/logical_insert.hpp"
#include "planner/operator/logical_limit.hpp"
#include "planner/operator/logical_order.hpp"
#include "planner/operator/logical_projection.hpp"
#include "planner/operator/logical_copy.hpp"

#include <map>

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

void LogicalPlanGenerator::Visit(AggregateExpression &expr) {
	SQLNodeVisitor::Visit(expr);
	// add cast if types don't match
	for (size_t i = 0; i < expr.children.size(); i++) {
		auto &child = expr.children[i];
		if (child->return_type != expr.return_type) {
			auto cast = make_unique<CastExpression>(expr.return_type,
													move(expr.children[i]));
			expr.children[i] = move(cast);
		}
	}
}

void LogicalPlanGenerator::Visit(ComparisonExpression &expr) {
	SQLNodeVisitor::Visit(expr);
	cast_children_to_equal_types(expr);
}

void LogicalPlanGenerator::Visit(ConjunctionExpression &expr) {
	SQLNodeVisitor::Visit(expr);
	cast_children_to_equal_types(expr);
}

void LogicalPlanGenerator::Visit(OperatorExpression &expr) {
	SQLNodeVisitor::Visit(expr);
	cast_children_to_equal_types(expr);
}

void LogicalPlanGenerator::Visit(SubqueryExpression &expr) {
	auto old_root = move(root);
	expr.subquery->Accept(this);
	if (!root) {
		throw Exception("Can't plan subquery");
	}

	expr.op = move(root);
	root = move(old_root);
}

void LogicalPlanGenerator::Visit(BaseTableRef &expr) {
	auto table = catalog.GetTable(expr.schema_name, expr.table_name);
	auto get_table = make_unique<LogicalGet>(
			table, expr.alias.empty() ? expr.table_name : expr.alias);
	if (root)
		get_table->children.push_back(move(root));
	root = move(get_table);
}

void LogicalPlanGenerator::Visit(CrossProductRef &expr) {
	throw NotImplementedException("Cross product not implemented yet!");
}

void LogicalPlanGenerator::Visit(JoinRef &expr) {
	throw NotImplementedException("Joins not implemented yet!");
}

void LogicalPlanGenerator::Visit(SubqueryRef &expr) {
	throw NotImplementedException("Joins not implemented yet!");
}

void LogicalPlanGenerator::Visit(InsertStatement &statement) {
	auto table = catalog.GetTable(statement.schema, statement.table);
	std::vector<std::unique_ptr<AbstractExpression>> insert_val_list;

	if (statement.columns.size() == 0) {
		if (statement.values.size() != table->columns.size()) {
			throw Exception("Not enough values for insert");
		}
		for (size_t i = 0; i < statement.values.size(); i++) {
			insert_val_list.push_back(move(statement.values[i]));
		}
	} else {
		if (statement.values.size() != statement.columns.size()) {
			throw Exception("Column name/value mismatch");
		}
		map<std::string, unique_ptr<AbstractExpression>> insert_vals;
		for (size_t i = 0; i < statement.values.size(); i++) {
			insert_vals[statement.columns[i]] = move(statement.values[i]);
		}
		for (auto col : table->columns) {
			if (insert_vals.count(col->name)) { // column value was specified
				insert_val_list.push_back(move(insert_vals[col->name]));
			} else {
				insert_val_list.push_back(std::unique_ptr<AbstractExpression>(
						new ConstantExpression(col->default_value)));
			}
		}
	}

	auto insert = make_unique<LogicalInsert>(table, move(insert_val_list));
	root = move(insert);
}

void LogicalPlanGenerator::Visit(CopyStatement &statement) {
	auto table = catalog.GetTable(statement.schema, statement.table);
	auto copy = make_unique<LogicalCopy>(table, move(statement.file_path),move(statement.is_from),move(statement.delimiter),move(statement.quote),move(statement.escape));
	root = move(copy);
}