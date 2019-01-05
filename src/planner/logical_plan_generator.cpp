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

unique_ptr<SQLStatement> LogicalPlanGenerator::Visit(SelectStatement &statement) {
	auto expected_column_count = statement.node->GetSelectCount();
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
		vector<unique_ptr<Expression>> projections;
		vector<unique_ptr<Expression>> groups;

		for (size_t i = 0; i < node->expressions.size(); i++) {
			Expression *proj_ele = node->expressions[i].get();

			groups.push_back(make_unique_base<Expression, ColumnRefExpression>(proj_ele->return_type, i));
			auto colref = make_unique_base<Expression, ColumnRefExpression>(proj_ele->return_type, i);
			colref->alias = proj_ele->alias;
			;
			projections.push_back(move(colref));
		}
		// this aggregate is superflous if all grouping columns are in aggr
		// below
		auto aggregate = make_unique<LogicalAggregate>(move(expressions));
		aggregate->groups = move(groups);
		aggregate->AddChild(move(root));
		root = move(aggregate);

		auto proj = make_unique<LogicalProjection>(move(projections));
		proj->AddChild(move(root));
		root = move(proj);
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

static unique_ptr<Expression> extract_aggregates(unique_ptr<Expression> expr, vector<unique_ptr<Expression>> &result,
                                                 size_t ngroups) {
	if (expr->GetExpressionClass() == ExpressionClass::AGGREGATE) {
		auto colref_expr = make_unique<ColumnRefExpression>(expr->return_type, ngroups + result.size());
		result.push_back(move(expr));
		return colref_expr;
	}
	expr->EnumerateChildren([&](unique_ptr<Expression> expr) -> unique_ptr<Expression> {
		return extract_aggregates(move(expr), result, ngroups);
	});
	return expr;
}

static unique_ptr<Expression> extract_windows(unique_ptr<Expression> expr, vector<unique_ptr<Expression>> &result,
                                              size_t ngroups) {
	if (expr->GetExpressionClass() == ExpressionClass::WINDOW) {
		auto colref_expr = make_unique<ColumnRefExpression>(expr->return_type, ngroups + result.size());
		result.push_back(move(expr));
		return colref_expr;
	}

	expr->EnumerateChildren([&](unique_ptr<Expression> expr) -> unique_ptr<Expression> {
		return extract_windows(move(expr), result, ngroups);
	});
	return expr;
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

	if (statement.HasAggregation()) {
		vector<unique_ptr<Expression>> aggregates;

		// TODO: what about the aggregates in window partition/order/boundaries? use a visitor here?
		for (size_t expr_idx = 0; expr_idx < statement.select_list.size(); expr_idx++) {
			statement.select_list[expr_idx] =
			    extract_aggregates(move(statement.select_list[expr_idx]), aggregates, statement.groupby.groups.size());
		}

		if (statement.HasHaving()) {
			AcceptChild(&statement.groupby.having);
			// the HAVING child cannot contain aggregates itself
			// turn them into Column References
			statement.groupby.having =
			    extract_aggregates(move(statement.groupby.having), aggregates, statement.groupby.groups.size());
		}

		auto aggregate = make_unique<LogicalAggregate>(move(aggregates));
		if (statement.HasGroup()) {
			// have to add group by columns
			aggregate->groups = move(statement.groupby.groups);
		}

		aggregate->AddChild(move(root));
		root = move(aggregate);

		if (statement.HasHaving()) {
			auto having = make_unique<LogicalFilter>(move(statement.groupby.having));

			having->AddChild(move(root));
			root = move(having);
		}
	}

	if (statement.HasWindow()) {
		auto win = make_unique<LogicalWindow>();
		for (size_t expr_idx = 0; expr_idx < statement.select_list.size(); expr_idx++) {
			// FIXME find a better way of getting colcount of logical ops
			root->ResolveOperatorTypes();
			statement.select_list[expr_idx] =
			    extract_windows(move(statement.select_list[expr_idx]), win->expressions, root->types.size());
		}
		assert(win->expressions.size() > 0);
		win->AddChild(move(root));
		root = move(win);
	}

	auto proj = make_unique<LogicalProjection>(move(statement.select_list));
	proj->AddChild(move(root));
	root = move(proj);
	VisitQueryNode(statement);
}

static unique_ptr<LogicalOperator> CastSetOpToTypes(vector<TypeId> &types, unique_ptr<LogicalOperator> op) {
	auto node = op.get();
	while (!IsProjection(node->type) && node->children.size() == 1) {
		node = node->children[0].get();
	}
	if (node->type == LogicalOperatorType::PROJECTION) {
		// found a projection node, we can just do the casts in there
		assert(node->expressions.size() == types.size());
		// add the casts to the selection list
		for (size_t i = 0; i < types.size(); i++) {
			if (node->expressions[i]->return_type != types[i]) {
				// differing types, have to add a cast
				node->expressions[i] = make_unique<CastExpression>(types[i], move(node->expressions[i]));
			}
		}
		return op;
	} else {
		// found a UNION or other set operator before we found a projection
		// we need to push a projection IF we need to do any casts
		// first check if we need to do any
		assert(node == op.get()); // if we don't encounter a projection the union should be the root node
		node->ResolveOperatorTypes();
		assert(types.size() == node->types.size());
		bool require_cast = false;
		for (size_t i = 0; i < types.size(); i++) {
			if (node->types[i] != types[i]) {
				require_cast = true;
				break;
			}
		}
		if (!require_cast) {
			// no cast required
			return op;
		}
		// need to perform a cast, push a projection
		vector<unique_ptr<Expression>> select_list;
		select_list.reserve(types.size());
		for (size_t i = 0; i < types.size(); i++) {
			unique_ptr<Expression> result = make_unique<ColumnRefExpression>(node->types[i], i);
			if (node->types[i] != types[i]) {
				result = make_unique<CastExpression>(types[i], move(result));
			}
			select_list.push_back(move(result));
		}
		auto projection = make_unique<LogicalProjection>(move(select_list));
		projection->children.push_back(move(op));
		projection->ResolveOperatorTypes();
		return move(projection);
	}
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
	// figure out the types of the setop result from the selection lists
	for (size_t i = 0; i < left_select_list.size(); i++) {
		Expression *proj_ele = left_select_list[i].get();

		TypeId union_expr_type = TypeId::INVALID;
		auto left_expr_type = proj_ele->return_type;
		auto right_expr_type = right_select_list[i]->return_type;
		// the type is the biggest of the two types
		// we might need to cast one of the sides
		union_expr_type = left_expr_type;
		if (right_expr_type > union_expr_type) {
			union_expr_type = right_expr_type;
		}

		union_types.push_back(union_expr_type);
	}

	// now visit the expressions to generate the plans
	statement.left->Accept(&generator_left);
	auto left_node = move(generator_left.root);

	statement.right->Accept(&generator_right);
	auto right_node = move(generator_right.root);

	assert(left_node);
	assert(right_node);

	// check for each node if we need to cast any of the columns
	left_node = CastSetOpToTypes(union_types, move(left_node));
	right_node = CastSetOpToTypes(union_types, move(right_node));

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

//! Get the combined type of a set of types
static TypeId GetMaxType(vector<TypeId> types) {
	TypeId result_type = TypeId::INVALID;
	for(auto &type : types) {
		if (type > result_type) {
			result_type = type;
		}
	}
	return result_type;
}

//! Add an optional cast to a set of types
static unique_ptr<Expression> AddCastToType(TypeId type, unique_ptr<Expression> expr) {
	if (expr->return_type != type) {
		return make_unique<CastExpression>(type, move(expr));
	}
	return expr;
}

unique_ptr<Expression> LogicalPlanGenerator::Visit(AggregateExpression &expr) {
	SQLNodeVisitor::Visit(expr);
	// add cast to child of aggregate if types don't match
	expr.child = AddCastToType(expr.return_type, move(expr.child));
	return nullptr;
}

unique_ptr<Expression> LogicalPlanGenerator::Visit(CaseExpression &expr) {
	SQLNodeVisitor::Visit(expr);
	// check needs to be bool
	expr.check = AddCastToType(TypeId::BOOLEAN, move(expr.check));
	// res_if_true and res_if_false need the same type
	auto result_type = GetMaxType({expr.result_if_true->return_type, expr.result_if_false->return_type});
	expr.result_if_true = AddCastToType(result_type, move(expr.result_if_true));
	expr.result_if_false = AddCastToType(result_type, move(expr.result_if_false));
	assert(result_type == expr.return_type);
	return nullptr;
}

unique_ptr<Expression> LogicalPlanGenerator::Visit(ComparisonExpression &expr) {
	SQLNodeVisitor::Visit(expr);
	if (expr.left->return_type != expr.right->return_type) {
		// add a cast if the types don't match
		auto result_type = GetMaxType({expr.left->return_type, expr.right->return_type});
		expr.left = AddCastToType(result_type, move(expr.left));
		expr.right = AddCastToType(result_type, move(expr.right));
	}
	return nullptr;
}

// TODO: this is ugly, generify functionality
unique_ptr<Expression> LogicalPlanGenerator::Visit(ConjunctionExpression &expr) {
	SQLNodeVisitor::Visit(expr);
	expr.left = AddCastToType(TypeId::BOOLEAN, move(expr.left));
	expr.right = AddCastToType(TypeId::BOOLEAN, move(expr.right));
	return nullptr;
}

unique_ptr<Expression> LogicalPlanGenerator::Visit(OperatorExpression &expr) {
	SQLNodeVisitor::Visit(expr);
	if (expr.type == ExpressionType::OPERATOR_NOT && expr.children[0]->return_type != TypeId::BOOLEAN) {
		expr.children[0] = AddCastToType(TypeId::BOOLEAN, move(expr.children[0]));
	} else {
		vector<TypeId> types;
		for(auto &child : expr.children) {
			types.push_back(child->return_type);
		}
		auto result_type = GetMaxType(types);
		for(size_t i = 0; i < expr.children.size(); i++) {
			expr.children[i] = AddCastToType(result_type, move(expr.children[i]));
		}
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
		throw Exception("Joins product cannot have children!");
	}

	// we do not generate joins here

	AcceptChild(&expr.left);
	assert(root);
	join->AddChild(move(root));
	root = nullptr;

	AcceptChild(&expr.right);
	assert(root);
	join->AddChild(move(root));
	root = nullptr;

	join->expressions.push_back(move(expr.condition));
	LogicalFilter::SplitPredicates(join->expressions);

	root = move(join);

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
	if (!statement.table.empty()) {
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
