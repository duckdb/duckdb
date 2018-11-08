
#include "planner/logical_plan_generator.hpp"

#include "parser/expression/list.hpp"
#include "parser/statement/list.hpp"

#include "parser/tableref/list.hpp"

#include "planner/operator/list.hpp"

#include "main/client_context.hpp"
#include "main/database.hpp"

#include <map>

using namespace duckdb;
using namespace std;

void LogicalPlanGenerator::Visit(CreateTableStatement &statement) {
	if (root) {
		throw Exception("CREATE TABLE from SELECT not supported yet!");
	}
	// bind the schema
	auto schema = context.db.catalog.GetSchema(context.ActiveTransaction(),
	                                           statement.info->schema);
	// create the logical operator
	root = make_unique<LogicalCreate>(schema, move(statement.info));
}

void LogicalPlanGenerator::Visit(UpdateStatement &statement) {
	// we require row ids for the deletion
	require_row_id = true;
	// create the table scan
	statement.table->Accept(this);
	if (!root || root->type != LogicalOperatorType::GET) {
		throw Exception("Cannot create update node without table scan!");
	}
	auto get = (LogicalGet *)root.get();
	// create the filter (if any)
	if (statement.condition) {
		statement.condition->Accept(this);
		auto filter = make_unique<LogicalFilter>(move(statement.condition));
		filter->AddChild(move(root));
		root = move(filter);
	}
	// scan the table for the referenced columns in the update clause
	auto &table = get->table;
	vector<column_t> column_ids;
	for (size_t i = 0; i < statement.columns.size(); i++) {
		auto &colname = statement.columns[i];

		if (!table->ColumnExists(colname)) {
			throw BinderException(
			    "Referenced update column %s not found in table!",
			    colname.c_str());
		}
		auto &column = table->GetColumn(colname);
		column_ids.push_back(column.oid);
		if (statement.expressions[i]->type == ExpressionType::VALUE_DEFAULT) {
			// resolve the type of the DEFAULT expression
			statement.expressions[i]->return_type = column.type;
		} else {
			// check if we have to create a cast
			if (statement.expressions[i]->return_type != column.type) {
				// differing types, create a cast
				statement.expressions[i] = make_unique<CastExpression>(
				    column.type, move(statement.expressions[i]));
			}
		}
	}
	// create the update node
	auto update = make_unique<LogicalUpdate>(table, column_ids,
	                                         move(statement.expressions));
	update->AddChild(move(root));
	root = move(update);
}

void LogicalPlanGenerator::Visit(DeleteStatement &statement) {
	// we require row ids for the deletion
	require_row_id = true;
	// create the table scan
	statement.table->Accept(this);
	if (!root || root->type != LogicalOperatorType::GET) {
		throw Exception("Cannot create delete node without table scan!");
	}
	auto get = (LogicalGet *)root.get();
	// create the filter (if any)
	if (statement.condition) {
		statement.condition->Accept(this);
		auto filter = make_unique<LogicalFilter>(move(statement.condition));
		filter->AddChild(move(root));
		root = move(filter);
	}
	// create the delete node
	auto del = make_unique<LogicalDelete>(get->table);
	del->AddChild(move(root));
	root = move(del);
}

static std::unique_ptr<Expression>
transform_aggregates_into_colref(LogicalOperator *root,
                                 std::unique_ptr<Expression> expr) {
	if (expr->GetExpressionClass() == ExpressionClass::AGGREGATE) {
		// the current expression is an aggregate node!
		// first check if the aggregate is already computed in the GROUP BY
		size_t match_index = root->expressions.size();
		for (size_t j = 0; j < root->expressions.size(); j++) {
			if (expr->Equals(root->expressions[j].get())) {
				match_index = j;
				break;
			}
		}
		auto type = expr->return_type;
		if (match_index == root->expressions.size()) {
			// the expression is not computed yet!
			// add it to the list of aggregates to compute
			root->expressions.push_back(move(expr));
		}
		// now turn this node into a reference
		return make_unique<ColumnRefExpression>(type, match_index);
	} else {
		for (size_t i = 0; i < expr->children.size(); i++) {
			expr->children[i] =
			    transform_aggregates_into_colref(root, move(expr->children[i]));
		}
		return expr;
	}
}

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

	size_t original_column_count = statement.select_list.size();
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
			// the HAVING child cannot contain aggregates itself
			// turn them into Column References
			for (size_t i = 0; i < having->expressions.size(); i++) {
				having->expressions[i] = transform_aggregates_into_colref(
				    root.get(), move(having->expressions[i]));
			}
			bool require_prune =
			    root->expressions.size() > original_column_count;
			having->AddChild(move(root));
			root = move(having);
			if (require_prune) {
				// we added extra aggregates for the HAVING clause
				// we need to prune them after
				auto prune =
				    make_unique<LogicalPruneColumns>(original_column_count);
				prune->AddChild(move(root));
				root = move(prune);
			}
		}
	} else {
		auto projection =
		    make_unique<LogicalProjection>(move(statement.select_list));
		projection->AddChild(move(root));
		root = move(projection);
	}

	if (statement.union_select) {
		auto top_node = move(root);
		statement.union_select->Accept(this);
		auto bottom_node = move(root);

		// get the projections
		auto top_select = GetProjection(top_node.get());
		auto bottom_select = GetProjection(bottom_node.get());

		if (!IsProjection(top_select->type) ||
		    !IsProjection(bottom_select->type)) {
			throw Exception(
			    "UNION can only apply to projection, union or group");
		}
		if (top_select->expressions.size() !=
		    bottom_select->expressions.size()) {
			throw Exception("UNION can only concatenate expressions with the "
			                "same number of result column");
		}

		vector<unique_ptr<Expression>> expressions;
		for (size_t i = 0; i < top_select->expressions.size(); i++) {
			Expression *proj_ele = top_select->expressions[i].get();

			TypeId union_expr_type = TypeId::INVALID;
			auto top_expr_type = proj_ele->return_type;
			auto bottom_expr_type = bottom_select->expressions[i]->return_type;
			union_expr_type = top_expr_type;
			if (bottom_expr_type > union_expr_type) {
				union_expr_type = bottom_expr_type;
			}
			if (top_expr_type != union_expr_type) {
				auto cast = make_unique<CastExpression>(
				    union_expr_type, move(top_select->expressions[i]));
				top_select->expressions[i] = move(cast);
			}
			if (bottom_expr_type != union_expr_type) {
				auto cast = make_unique<CastExpression>(
				    union_expr_type, move(bottom_select->expressions[i]));
				bottom_select->expressions[i] = move(cast);
			}

			auto ele_ref = make_unique_base<Expression, ColumnRefExpression>(
			    union_expr_type, i);
			ele_ref->alias = proj_ele->alias;
			expressions.push_back(move(ele_ref));
		}

		auto union_op =
		    make_unique<LogicalUnion>(move(top_node), move(bottom_node));
		union_op->expressions = move(expressions);

		root = move(union_op);
	}

	if (statement.select_distinct) {
		auto node = GetProjection(root.get());
		if (!IsProjection(node->type)) {
			throw Exception(
			    "DISTINCT can only apply to projection, union or group");
		}

		vector<unique_ptr<Expression>> expressions;
		vector<unique_ptr<Expression>> groups;

		for (size_t i = 0; i < node->expressions.size(); i++) {
			Expression *proj_ele = node->expressions[i].get();
			auto group_ref = make_unique_base<Expression, GroupRefExpression>(
			    proj_ele->return_type, i);
			group_ref->alias = proj_ele->alias;
			expressions.push_back(move(group_ref));
			groups.push_back(make_unique_base<Expression, ColumnRefExpression>(
			    proj_ele->return_type, i));
		}
		// this aggregate is superflous if all grouping columns are in aggr
		// below
		auto aggregate = make_unique<LogicalAggregate>(move(expressions));
		aggregate->groups = move(groups);
		aggregate->AddChild(move(root));
		root = move(aggregate);
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
	if (!is_subquery) {
		// always prune the root node
		auto prune =
		    make_unique<LogicalPruneColumns>(statement.result_column_count);
		prune->AddChild(move(root));
		root = move(prune);
	}
}

static void cast_children_to_equal_types(Expression &expr,
                                         size_t start_idx = 0) {
	// first figure out the widest type
	TypeId max_type = TypeId::INVALID;
	for (size_t child_idx = start_idx; child_idx < expr.children.size();
	     child_idx++) {
		TypeId child_type = expr.children[child_idx]->return_type;
		if (child_type > max_type) {
			max_type = child_type;
		}
	}
	// now add casts where appropriate
	for (size_t child_idx = start_idx; child_idx < expr.children.size();
	     child_idx++) {
		TypeId child_type = expr.children[child_idx]->return_type;
		if (child_type != max_type) {
			auto cast = make_unique<CastExpression>(
			    max_type, move(expr.children[child_idx]));
			expr.children[child_idx] = move(cast);
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

void LogicalPlanGenerator::Visit(CaseExpression &expr) {
	SQLNodeVisitor::Visit(expr);
	assert(expr.children.size() == 3);
	// check needs to be bool
	if (expr.children[0]->return_type != TypeId::BOOLEAN) {
		auto cast = make_unique<CastExpression>(TypeId::BOOLEAN,
		                                        move(expr.children[0]));
		expr.children[0] = move(cast);
	}
	// others need same type
	cast_children_to_equal_types(expr, 1);
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
	if (expr.type == ExpressionType::OPERATOR_NOT &&
	    expr.children[0]->return_type != TypeId::BOOLEAN) {
		auto cast = make_unique<CastExpression>(TypeId::BOOLEAN,
		                                        move(expr.children[0]));
		expr.children[0] = move(cast);
	} else {
		cast_children_to_equal_types(expr);
	}
}

void LogicalPlanGenerator::Visit(SubqueryExpression &expr) {
	LogicalPlanGenerator generator(context, *expr.context, true);
	expr.subquery->Accept(&generator);
	if (!generator.root) {
		throw Exception("Can't plan subquery");
	}
	expr.op = move(generator.root);
	assert(expr.op);
}

void LogicalPlanGenerator::Visit(BaseTableRef &expr) {
	// FIXME: catalog access should only happen once in binder
	auto table = context.db.catalog.GetTable(context.ActiveTransaction(),
	                                         expr.schema_name, expr.table_name);
	auto alias = expr.alias.empty() ? expr.table_name : expr.alias;

	auto index = bind_context.GetBindingIndex(alias);

	std::vector<column_t> column_ids;
	// look in the context for this table which columns are required
	for (auto &bound_column : bind_context.bound_columns[alias]) {
		column_ids.push_back(table->name_map[bound_column]);
	}
	if (require_row_id || column_ids.size() == 0) {
		// no column ids selected
		// the query is like SELECT COUNT(*) FROM table, or SELECT 42 FROM table
		// return just the row id
		column_ids.push_back(COLUMN_IDENTIFIER_ROW_ID);
	}

	auto get_table = make_unique<LogicalGet>(table, index, column_ids);
	if (root) {
		get_table->AddChild(move(root));
	}
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
	expr.condition->Accept(this);
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
	if (join->conditions.size() > 0) {
		root = move(join);
	} else { // the conditions were not comparisions between left on right,
		     // cross product
		auto cp = make_unique<LogicalCrossProduct>();
		cp->AddChild(move(join->children[0]));
		cp->AddChild(move(join->children[1]));
		root = move(cp);
	}
}

void LogicalPlanGenerator::Visit(SubqueryRef &expr) {
	// generate the logical plan for the subquery
	// this happens separately from the current LogicalPlan generation
	LogicalPlanGenerator generator(context, *expr.context, true);

	size_t column_count = expr.subquery->select_list.size();
	expr.subquery->Accept(&generator);

	auto index = bind_context.GetBindingIndex(expr.alias);

	if (root) {
		throw Exception("Subquery cannot have children");
	}
	root = make_unique<LogicalSubquery>(index, column_count);
	// this intentionally does a push_back directly instead of using AddChild
	// because the Subquery is stand-alone, we do not copy the referenced_tables
	// of the children
	root->children.push_back(move(generator.root));
}

void LogicalPlanGenerator::Visit(TableFunction &expr) {
	// FIXME: catalog access should only happen once in binder
	auto function_definition = (FunctionExpression *)expr.function.get();
	auto function = context.db.catalog.GetTableFunction(
	    context.ActiveTransaction(), function_definition);

	auto index = bind_context.GetBindingIndex(
	    expr.alias.empty() ? function_definition->function_name : expr.alias);

	if (root) {
		throw Exception("Table function cannot have children");
	}
	root =
	    make_unique<LogicalTableFunction>(function, index, move(expr.function));
}

void LogicalPlanGenerator::Visit(InsertStatement &statement) {
	auto table = context.db.catalog.GetTable(context.ActiveTransaction(),
	                                         statement.schema, statement.table);
	auto insert = make_unique<LogicalInsert>(table);

	if (statement.columns.size() > 0) {
		// insertion statement specifies column list

		// create a mapping of (list index) -> (column index)
		map<std::string, int> column_name_map;
		for (size_t i = 0; i < statement.columns.size(); i++) {
			column_name_map[statement.columns[i]] = i;
		}
		for (size_t i = 0; i < table->columns.size(); i++) {
			auto &col = table->columns[i];
			auto entry = column_name_map.find(col.name);
			if (entry == column_name_map.end()) {
				// column not specified, set index to -1
				insert->column_index_map.push_back(-1);
			} else {
				// column was specified, set to the index
				insert->column_index_map.push_back(entry->second);
			}
		}
	}

	if (statement.select_statement) {
		// insert from select statement
		// parse select statement and add to logical plan
		statement.select_statement->Accept(this);
		assert(root);
		insert->AddChild(move(root));
		root = move(insert);
	} else {
		// first visit the expressions
		for (auto &expression_list : statement.values) {
			for (auto &expression : expression_list) {
				expression->Accept(this);
			}
		}
		// insert from constants
		// check if the correct amount of constants are supplied
		if (statement.columns.size() == 0) {
			if (statement.values[0].size() != table->columns.size()) {
				throw SyntaxException(
				    "table %s has %d columns but %d values were supplied",
				    table->name.c_str(), table->columns.size(),
				    statement.values[0].size());
			}
		} else {
			if (statement.values[0].size() != statement.columns.size()) {
				throw SyntaxException(
				    "Column name/value mismatch: %d values for %d columns",
				    statement.values[0].size(), statement.columns.size());
			}
		}
		insert->insert_values = move(statement.values);
		root = move(insert);
	}
}

void LogicalPlanGenerator::Visit(CopyStatement &statement) {
	if (statement.table[0] != '\0') {
		auto table = context.db.catalog.GetTable(
		    context.ActiveTransaction(), statement.schema, statement.table);
		auto copy = make_unique<LogicalCopy>(
		    table, move(statement.file_path), move(statement.is_from),
		    move(statement.delimiter), move(statement.quote),
		    move(statement.escape), move(statement.select_list));
		root = move(copy);
	} else {
		auto copy = make_unique<LogicalCopy>(
		    move(statement.file_path), move(statement.is_from),
		    move(statement.delimiter), move(statement.quote),
		    move(statement.escape));
		statement.select_statement->Accept(this);
		assert(root);
		copy->AddChild(move(root));
		root = move(copy);
	}
}
