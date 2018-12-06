#include "planner/logical_plan_generator.hpp"

#include "main/client_context.hpp"
#include "main/database.hpp"
#include "parser/expression/list.hpp"
#include "parser/query_node/list.hpp"
#include "parser/statement/list.hpp"
#include "parser/tableref/list.hpp"
#include "planner/operator/list.hpp"

#include <map>

using namespace duckdb;
using namespace std;

unique_ptr<SQLStatement> LogicalPlanGenerator::Visit(CreateTableStatement &statement) {
	if (root) {
		throw Exception("CREATE TABLE from SELECT not supported yet!");
	}
	// bind the schema
	auto schema = context.db.catalog.GetSchema(context.ActiveTransaction(), statement.info->schema);
	// create the logical operator
	root = make_unique<LogicalCreate>(schema, move(statement.info));
	return nullptr;
}

unique_ptr<SQLStatement> LogicalPlanGenerator::Visit(CreateIndexStatement &statement) {
	// first we visit the base table
	statement.table->Accept(this);
	// this gives us a logical table scan
	// we take the required columns from here
	assert(root && root->type == LogicalOperatorType::GET);
	auto get = (LogicalGet *)root.get();
	auto column_ids = get->column_ids;

	// bind the table
	auto table = context.db.catalog.GetTable(context.ActiveTransaction(), statement.table->schema_name,
	                                         statement.table->table_name);
	// create the logical operator
	root = make_unique<LogicalCreateIndex>(*table, column_ids, move(statement.expressions), move(statement.info));
	return nullptr;
}

unique_ptr<SQLStatement> LogicalPlanGenerator::Visit(UpdateStatement &statement) {
	// we require row ids for the deletion
	require_row_id = true;
	// create the table scan
	AcceptChild(&statement.table);
	if (!root || root->type != LogicalOperatorType::GET) {
		throw Exception("Cannot create update node without table scan!");
	}
	auto get = (LogicalGet *)root.get();
	// create the filter (if any)
	if (statement.condition) {
		AcceptChild(&statement.condition);
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
			throw BinderException("Referenced update column %s not found in table!", colname.c_str());
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
				statement.expressions[i] = make_unique<CastExpression>(column.type, move(statement.expressions[i]));
			}
		}
	}
	// create the update node
	auto update = make_unique<LogicalUpdate>(table, column_ids, move(statement.expressions));
	update->AddChild(move(root));
	root = move(update);
	return nullptr;
}

unique_ptr<SQLStatement> LogicalPlanGenerator::Visit(DeleteStatement &statement) {
	// we require row ids for the deletion
	require_row_id = true;
	// create the table scan
	AcceptChild(&statement.table);
	if (!root || root->type != LogicalOperatorType::GET) {
		throw Exception("Cannot create delete node without table scan!");
	}
	auto get = (LogicalGet *)root.get();
	// create the filter (if any)
	if (statement.condition) {
		AcceptChild(&statement.condition);
		auto filter = make_unique<LogicalFilter>(move(statement.condition));
		filter->AddChild(move(root));
		root = move(filter);
	}
	// create the delete node
	auto del = make_unique<LogicalDelete>(get->table);
	del->AddChild(move(root));
	root = move(del);
	return nullptr;
}

static unique_ptr<Expression> transform_aggregates_into_colref(LogicalOperator *root, unique_ptr<Expression> expr) {
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
			expr->children[i] = transform_aggregates_into_colref(root, move(expr->children[i]));
		}
		return expr;
	}
}

unique_ptr<SQLStatement> LogicalPlanGenerator::Visit(SelectStatement &statement) {
	auto expected_column_count = statement.node->GetSelectList().size();
	statement.node->Accept(this);
	// prune the root node
	assert(root);
	auto prune = make_unique<LogicalPruneColumns>(expected_column_count);
	prune->AddChild(move(root));
	root = move(prune);
	return nullptr;
}

void LogicalPlanGenerator::VisitQueryNode(QueryNode &statement) {
	assert(root);
	if (statement.select_distinct) {
		auto node = GetProjection(root.get());
		if (!IsProjection(node->type)) {
			throw Exception("DISTINCT can only apply to projection, union or group");
		}

		vector<unique_ptr<Expression>> expressions;
		vector<unique_ptr<Expression>> groups;

		for (size_t i = 0; i < node->expressions.size(); i++) {
			Expression *proj_ele = node->expressions[i].get();
			auto group_ref = make_unique_base<Expression, GroupRefExpression>(proj_ele->return_type, i);
			group_ref->alias = proj_ele->alias;
			expressions.push_back(move(group_ref));
			groups.push_back(make_unique_base<Expression, ColumnRefExpression>(proj_ele->return_type, i));
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
		auto limit = make_unique<LogicalLimit>(statement.limit.limit, statement.limit.offset);
		limit->AddChild(move(root));
		root = move(limit);
	}
}

void LogicalPlanGenerator::Visit(SelectNode &statement) {
	for (auto &expr : statement.select_list) {
		AcceptChild(&expr);
	}

	if (statement.from_table) {
		// SELECT with FROM
		AcceptChild(&statement.from_table);
	} else {
		// SELECT without FROM, add empty GET
		root = make_unique<LogicalGet>();
	}

	if (statement.where_clause) {
		AcceptChild(&statement.where_clause);

		auto filter = make_unique<LogicalFilter>(move(statement.where_clause));
		filter->AddChild(move(root));
		root = move(filter);
	}

	// FIXME: these need to get pulled out because the list is lost in the move
	bool has_aggr = statement.HasAggregation();
	bool has_window = statement.HasWindow();

	// separate projection/aggr and window functions into two selection lists if req.
	auto select_list = move(statement.select_list);
	vector<unique_ptr<Expression>> window_select_list;
	if (has_window) {
		window_select_list.resize(select_list.size());

		for (size_t expr_idx = 0; expr_idx < select_list.size(); expr_idx++) {
			if (select_list[expr_idx]->GetExpressionClass() == ExpressionClass::WINDOW) {
				window_select_list[expr_idx] = move(select_list[expr_idx]);
				// TODO: does this need to be a groupref if we have an aggr below?
				select_list[expr_idx] = make_unique_base<Expression, ColumnRefExpression>(window_select_list[expr_idx]->return_type, expr_idx);
			} else {
				// leave select_list alone
				window_select_list[expr_idx] = make_unique_base<Expression, ColumnRefExpression>(select_list[expr_idx]->return_type, expr_idx);
			}
		}
	}



	size_t original_column_count = select_list.size();
	if (has_aggr) {
		auto aggregate = make_unique<LogicalAggregate>(move(select_list));
		if (statement.HasGroup()) {
			// have to add group by columns
			aggregate->groups = move(statement.groupby.groups);
		}

		aggregate->AddChild(move(root));
		root = move(aggregate);

		if (statement.HasHaving()) {
			AcceptChild(&statement.groupby.having);
			auto having = make_unique<LogicalFilter>(move(statement.groupby.having));
			// the HAVING child cannot contain aggregates itself
			// turn them into Column References
			for (size_t i = 0; i < having->expressions.size(); i++) {
				having->expressions[i] = transform_aggregates_into_colref(root.get(), move(having->expressions[i]));
			}
			bool require_prune = root->expressions.size() > original_column_count;
			having->AddChild(move(root));
			root = move(having);
			if (require_prune) {
				// we added extra aggregates for the HAVING clause
				// we need to prune them after
				auto prune = make_unique<LogicalPruneColumns>(original_column_count);
				prune->AddChild(move(root));
				root = move(prune);
			}
		}
	} else if (statement.select_list.size() > 0) {
		auto projection = make_unique<LogicalProjection>(move(select_list));
		projection->AddChild(move(root));
		root = move(projection);
	}

	// window statements here
	if (has_window) {
		assert(window_select_list.size() > 0);
		auto window = make_unique<LogicalWindow>(move(window_select_list));
		window->AddChild(move(root));
		root = move(window);
	}

	VisitQueryNode(statement);
}

void LogicalPlanGenerator::Visit(SetOperationNode &statement) {
	// Generate the logical plan for the left and right sides of the set operation
	LogicalPlanGenerator generator_left(context, *statement.setop_left_binder);
	LogicalPlanGenerator generator_right(context, *statement.setop_right_binder);

	// get the projections
	auto &left_select_list = statement.left->GetSelectList();
	auto &right_select_list = statement.right->GetSelectList();
	if (left_select_list.size() != right_select_list.size()) {
		throw Exception("Set operations can only apply to expressions with the "
		                "same number of result columns");
	}

	vector<TypeId> union_types;
	// figure out types of setop result
	for (size_t i = 0; i < left_select_list.size(); i++) {
		Expression *proj_ele = left_select_list[i].get();

		TypeId union_expr_type = TypeId::INVALID;
		auto left_expr_type = proj_ele->return_type;
		auto right_expr_type = right_select_list[i]->return_type;
		union_expr_type = left_expr_type;
		if (right_expr_type > union_expr_type) {
			union_expr_type = right_expr_type;
		}

		union_types.push_back(union_expr_type);
	}

	// project setop children to produce common type
	// FIXME duplicated code
	vector<unique_ptr<Expression>> proj_exprs_left;
	for (size_t i = 0; i < union_types.size(); i++) {
		auto ref_expr = make_unique<ColumnRefExpression>(left_select_list[i]->return_type, i);
		auto cast_expr = make_unique_base<Expression, CastExpression>(union_types[i], move(ref_expr));
		proj_exprs_left.push_back(move(cast_expr));
	}
	vector<unique_ptr<Expression>> proj_exprs_right;
	for (size_t i = 0; i < union_types.size(); i++) {
		auto ref_expr = make_unique<ColumnRefExpression>(right_select_list[i]->return_type, i);
		auto cast_expr = make_unique_base<Expression, CastExpression>(union_types[i], move(ref_expr));
		proj_exprs_right.push_back(move(cast_expr));
	}

	// now visit the expressions
	statement.left->Accept(&generator_left);
	auto left_node = move(generator_left.root);

	statement.right->Accept(&generator_right);
	auto right_node = move(generator_right.root);

	assert(left_node);
	assert(right_node);

	// create a dummy projection on both sides to hold the casts
	auto projection_left = make_unique<LogicalProjection>(move(proj_exprs_left));
	projection_left->children.push_back(move(left_node));
	left_node = move(projection_left);

	auto projection_right = make_unique<LogicalProjection>(move(proj_exprs_right));
	projection_right->children.push_back(move(right_node));
	right_node = move(projection_right);

	// create actual logical ops for setops
	switch (statement.setop_type) {
	case SetOperationType::UNION: {
		auto union_op = make_unique<LogicalUnion>(move(left_node), move(right_node));
		root = move(union_op);
		break;
	}
	case SetOperationType::EXCEPT: {
		auto except_op = make_unique<LogicalExcept>(move(left_node), move(right_node));
		root = move(except_op);
		break;
	}
	case SetOperationType::INTERSECT: {
		auto intersect_op = make_unique<LogicalIntersect>(move(left_node), move(right_node));
		root = move(intersect_op);
		break;
	}
	default:
		throw NotImplementedException("Set Operation type");
	}

	VisitQueryNode(statement);
}

static void cast_children_to_equal_types(Expression &expr, size_t start_idx = 0) {
	// first figure out the widest type
	TypeId max_type = expr.return_type;
	for (size_t i = start_idx; i < expr.children.size(); i++) {
		TypeId child_type = expr.children[i]->return_type;
		if (child_type > max_type) {
			max_type = child_type;
		}
	}
	// now add casts where appropriate
	for (size_t i = start_idx; i < expr.children.size(); i++) {
		TypeId child_type = expr.children[i]->return_type;
		if (child_type != max_type) {
			auto cast = make_unique<CastExpression>(max_type, move(expr.children[i]));
			cast->ResolveType();
			expr.children[i] = move(cast);
		}
	}
}

unique_ptr<Expression> LogicalPlanGenerator::Visit(AggregateExpression &expr) {
	SQLNodeVisitor::Visit(expr);
	// add cast if types don't match
	for (size_t i = 0; i < expr.children.size(); i++) {
		auto &child = expr.children[i];
		if (child->return_type != expr.return_type) {
			auto cast = make_unique<CastExpression>(expr.return_type, move(expr.children[i]));
			expr.children[i] = move(cast);
		}
	}
	return nullptr;
}

unique_ptr<Expression> LogicalPlanGenerator::Visit(CaseExpression &expr) {
	SQLNodeVisitor::Visit(expr);
	assert(expr.children.size() == 3);
	// check needs to be bool
	if (expr.children[0]->return_type != TypeId::BOOLEAN) {
		auto cast = make_unique<CastExpression>(TypeId::BOOLEAN, move(expr.children[0]));
		expr.children[0] = move(cast);
	}
	// others need same type
	cast_children_to_equal_types(expr, 1);
	return nullptr;
}

unique_ptr<Expression> LogicalPlanGenerator::Visit(ComparisonExpression &expr) {
	SQLNodeVisitor::Visit(expr);
	cast_children_to_equal_types(expr);
	return nullptr;
}

// TODO: this is ugly, generify functionality
unique_ptr<Expression> LogicalPlanGenerator::Visit(ConjunctionExpression &expr) {
	SQLNodeVisitor::Visit(expr);

	if (expr.children[0]->return_type != TypeId::BOOLEAN) {
		auto cast = make_unique<CastExpression>(TypeId::BOOLEAN, move(expr.children[0]));
		expr.children[0] = move(cast);
	}
	if (expr.children[1]->return_type != TypeId::BOOLEAN) {
		auto cast = make_unique<CastExpression>(TypeId::BOOLEAN, move(expr.children[1]));
		expr.children[1] = move(cast);
	}
	return nullptr;
}

unique_ptr<Expression> LogicalPlanGenerator::Visit(OperatorExpression &expr) {
	SQLNodeVisitor::Visit(expr);
	if (expr.type == ExpressionType::OPERATOR_NOT && expr.children[0]->return_type != TypeId::BOOLEAN) {
		auto cast = make_unique<CastExpression>(TypeId::BOOLEAN, move(expr.children[0]));
		expr.children[0] = move(cast);
	} else {
		cast_children_to_equal_types(expr);
	}
	return nullptr;
}

unique_ptr<Expression> LogicalPlanGenerator::Visit(SubqueryExpression &expr) {
	LogicalPlanGenerator generator(context, *expr.context);
	expr.subquery->Accept(&generator);
	if (!generator.root) {
		throw Exception("Can't plan subquery");
	}
	expr.op = move(generator.root);
	assert(expr.op);
	return nullptr;
}

unique_ptr<TableRef> LogicalPlanGenerator::Visit(BaseTableRef &expr) {
	// FIXME: catalog access should only happen once in binder
	auto table = context.db.catalog.GetTable(context.ActiveTransaction(), expr.schema_name, expr.table_name);
	auto alias = expr.alias.empty() ? expr.table_name : expr.alias;

	auto index = bind_context.GetBindingIndex(alias);

	vector<column_t> column_ids;
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
	return nullptr;
}

unique_ptr<TableRef> LogicalPlanGenerator::Visit(CrossProductRef &expr) {
	auto cross_product = make_unique<LogicalCrossProduct>();

	if (root) {
		throw Exception("Cross product cannot have children!");
	}

	AcceptChild(&expr.left);
	assert(root);
	cross_product->AddChild(move(root));
	root = nullptr;

	AcceptChild(&expr.right);
	assert(root);
	cross_product->AddChild(move(root));
	root = nullptr;

	root = move(cross_product);
	return nullptr;
}

unique_ptr<TableRef> LogicalPlanGenerator::Visit(JoinRef &expr) {
	AcceptChild(&expr.condition);
	auto join = make_unique<LogicalJoin>(expr.type);

	if (root) {
		throw Exception("Cross product cannot have children!");
	}

	AcceptChild(&expr.left);
	assert(root);
	join->AddChild(move(root));
	root = nullptr;

	AcceptChild(&expr.right);
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
	return nullptr;
}

unique_ptr<TableRef> LogicalPlanGenerator::Visit(SubqueryRef &expr) {
	// generate the logical plan for the subquery
	// this happens separately from the current LogicalPlan generation
	LogicalPlanGenerator generator(context, *expr.context);

	size_t column_count = expr.subquery->GetSelectList().size();
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
	return nullptr;
}

unique_ptr<TableRef> LogicalPlanGenerator::Visit(TableFunction &expr) {
	// FIXME: catalog access should only happen once in binder
	auto function_definition = (FunctionExpression *)expr.function.get();
	auto function = context.db.catalog.GetTableFunction(context.ActiveTransaction(), function_definition);

	auto index = bind_context.GetBindingIndex(expr.alias.empty() ? function_definition->function_name : expr.alias);

	if (root) {
		throw Exception("Table function cannot have children");
	}
	root = make_unique<LogicalTableFunction>(function, index, move(expr.function));
	return nullptr;
}

unique_ptr<SQLStatement> LogicalPlanGenerator::Visit(InsertStatement &statement) {
	auto table = context.db.catalog.GetTable(context.ActiveTransaction(), statement.schema, statement.table);
	auto insert = make_unique<LogicalInsert>(table);

	if (statement.columns.size() > 0) {
		// insertion statement specifies column list

		// create a mapping of (list index) -> (column index)
		map<string, int> column_name_map;
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
		AcceptChild(&statement.select_statement);
		assert(root);
		insert->AddChild(move(root));
		root = move(insert);
	} else {
		// first visit the expressions
		for (auto &expression_list : statement.values) {
			for (auto &expression : expression_list) {
				AcceptChild(&expression);
			}
		}
		// insert from constants
		// check if the correct amount of constants are supplied
		if (statement.columns.size() == 0) {
			if (statement.values[0].size() != table->columns.size()) {
				throw SyntaxException("table %s has %d columns but %d values were supplied", table->name.c_str(),
				                      table->columns.size(), statement.values[0].size());
			}
		} else {
			if (statement.values[0].size() != statement.columns.size()) {
				throw SyntaxException("Column name/value mismatch: %d values for %d columns",
				                      statement.values[0].size(), statement.columns.size());
			}
		}
		insert->insert_values = move(statement.values);
		root = move(insert);
	}
	return nullptr;
}

unique_ptr<SQLStatement> LogicalPlanGenerator::Visit(CopyStatement &statement) {
	if (statement.table[0] != '\0') {
		auto table = context.db.catalog.GetTable(context.ActiveTransaction(), statement.schema, statement.table);
		auto copy = make_unique<LogicalCopy>(table, move(statement.file_path), move(statement.is_from),
		                                     move(statement.delimiter), move(statement.quote), move(statement.escape),
		                                     move(statement.select_list));
		root = move(copy);
	} else {
		auto copy = make_unique<LogicalCopy>(move(statement.file_path), move(statement.is_from),
		                                     move(statement.delimiter), move(statement.quote), move(statement.escape));
		statement.select_statement->Accept(this);
		assert(root);
		copy->AddChild(move(root));
		root = move(copy);
	}
	return nullptr;
}
