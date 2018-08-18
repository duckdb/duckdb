
#include "planner/logical_plan_generator.hpp"

#include "parser/expression/expression_list.hpp"
#include "parser/statement/copy_statement.hpp"
#include "parser/statement/insert_statement.hpp"
#include "parser/tableref/tableref_list.hpp"

#include "planner/operator/logical_list.hpp"

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
		filter->AddChild(move(root));
		root = move(filter);
	}

	if (statement.HasAggregation()) {
		auto aggregate =
		    make_unique<LogicalAggregate>(move(statement.select_list));
		if (statement.HasGroup()) {
			// have to add group by columns
			aggregate->groups = move(statement.groupby.groups);
		}
		aggregate->AddChild(move(root));
		root = move(aggregate);

		if (statement.HasHaving()) {
			statement.groupby.having->Accept(this);

			auto having =
			    make_unique<LogicalFilter>(move(statement.groupby.having));
			having->AddChild(move(root));
			root = move(having);
		}
	} else {
		auto projection =
		    make_unique<LogicalProjection>(move(statement.select_list));
		projection->AddChild(move(root));
		root = move(projection);
	}

	if (statement.select_distinct) {
		auto distinct = make_unique<LogicalDistinct>();
		distinct->AddChild(move(root));
		root = move(distinct);
	}
	if (statement.HasOrder()) {
		auto order = make_unique<LogicalOrder>(move(statement.orderby));
		order->AddChild(move(root));
		root = move(order);
	}
	if (statement.HasLimit()) {
		auto limit = make_unique<LogicalLimit>(statement.limit.limit,
		                                       statement.limit.offset);
		limit->AddChild(move(root));
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

// TODO: this is ugly, generify functionality
void LogicalPlanGenerator::Visit(ConjunctionExpression &expr) {
	SQLNodeVisitor::Visit(expr);

	if (expr.children[0]->return_type != TypeId::BOOLEAN) {
		auto cast = make_unique<CastExpression>(TypeId::BOOLEAN,
		                                        move(expr.children[0]));
		expr.children[0] = move(cast);
	}
	if (expr.children[1]->return_type != TypeId::BOOLEAN) {
		auto cast = make_unique<CastExpression>(TypeId::BOOLEAN,
		                                        move(expr.children[1]));
		expr.children[1] = move(cast);
	}
}

void LogicalPlanGenerator::Visit(OperatorExpression &expr) {
	SQLNodeVisitor::Visit(expr);
	if (expr.type == ExpressionType::OPERATOR_NOT) {
		auto cast = make_unique<CastExpression>(TypeId::BOOLEAN,
		                                        move(expr.children[0]));
		expr.children[0] = move(cast);
	} else {
		cast_children_to_equal_types(expr);
	}
}

void LogicalPlanGenerator::Visit(SubqueryExpression &expr) {
	LogicalPlanGenerator generator(catalog, *expr.context);
	expr.subquery->Accept(&generator);
	if (!generator.root) {
		throw Exception("Can't plan subquery");
	}
	expr.op = move(generator.root);
}

void LogicalPlanGenerator::Visit(BaseTableRef &expr) {
	auto table = catalog.GetTable(expr.schema_name, expr.table_name);
	auto alias = expr.alias.empty() ? expr.table_name : expr.alias;

	auto index = context.GetTableIndex(alias);
	
	std::vector<size_t> column_ids;
	// look in the context for this table which columns are required
	for (auto &bound_column : context.bound_columns[alias]) {
		column_ids.push_back(table->name_map[bound_column]);
	}
	if (column_ids.size() == 0) {
		// no column ids selected
		// the query is like SELECT COUNT(*) FROM table, or SELECT 42 FROM table
		// return just the first column
		column_ids.push_back(0);
	}

	auto get_table = make_unique<LogicalGet>(table, alias, index, column_ids);
	if (root)
		get_table->AddChild(move(root));
	root = move(get_table);
}

void LogicalPlanGenerator::Visit(CrossProductRef &expr) {
	auto cross_product = make_unique<LogicalCrossProduct>();

	if (root) {
		throw Exception("Cross product cannot have children!");
	}

	expr.left->Accept(this);
	assert(root);
	cross_product->AddChild(move(root));
	root = nullptr;

	expr.right->Accept(this);
	assert(root);
	cross_product->AddChild(move(root));
	root = nullptr;

	root = move(cross_product);
}

void LogicalPlanGenerator::Visit(JoinRef &expr) {
	auto join = make_unique<LogicalJoin>(expr.type);

	if (root) {
		throw Exception("Cross product cannot have children!");
	}

	expr.left->Accept(this);
	assert(root);
	join->AddChild(move(root));
	root = nullptr;

	expr.right->Accept(this);
	assert(root);
	join->AddChild(move(root));
	root = nullptr;

	join->SetJoinCondition(move(expr.condition));

	root = move(join);
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
	if (statement.table[0] != '\0') {
		auto table = catalog.GetTable(statement.schema, statement.table);
		auto copy = make_unique<LogicalCopy>(
		    table, move(statement.file_path), move(statement.is_from),
		    move(statement.delimiter), move(statement.quote),
		    move(statement.escape));
		root = move(copy);
	} else {
		auto copy = make_unique<LogicalCopy>(
		    move(statement.file_path), move(statement.is_from),
		    move(statement.delimiter), move(statement.quote),
		    move(statement.escape));
		statement.select_stmt->Accept(this);
		copy->AddChild(move(root));
		root = move(copy);
	}
}
