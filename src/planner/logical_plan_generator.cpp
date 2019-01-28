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

void LogicalPlanGenerator::CreatePlan(SQLStatement &statement) {
	switch (statement.type) {
	case StatementType::SELECT:
		CreatePlan((SelectStatement &)statement);
		break;
	case StatementType::INSERT:
		CreatePlan((InsertStatement &)statement);
		break;
	case StatementType::COPY:
		CreatePlan((CopyStatement &)statement);
		break;
	case StatementType::DELETE:
		CreatePlan((DeleteStatement &)statement);
		break;
	case StatementType::UPDATE:
		CreatePlan((UpdateStatement &)statement);
		break;
	case StatementType::ALTER:
		CreatePlan((AlterTableStatement &)statement);
		break;
	case StatementType::CREATE_TABLE:
		CreatePlan((CreateTableStatement &)statement);
		break;
	case StatementType::CREATE_INDEX:
		CreatePlan((CreateIndexStatement &)statement);
		break;
	default:
		throw NotImplementedException("Statement type");
		break;
	}
}

void LogicalPlanGenerator::CreatePlan(QueryNode &node) {
	if (node.type == QueryNodeType::SELECT_NODE) {
		CreatePlan((SelectNode &)node);
	} else {
		assert(node.type == QueryNodeType::SET_OPERATION_NODE);
		CreatePlan((SetOperationNode &)node);
	}
}

//! Get the combined type of a set of types
static TypeId GetMaxType(vector<TypeId> types) {
	TypeId result_type = TypeId::INVALID;
	for (auto &type : types) {
		if (type > result_type) {
			result_type = type;
		}
	}
	return result_type;
}

//! Add an optional cast to a set of types
static unique_ptr<Expression> AddCastToType(TypeId type, unique_ptr<Expression> expr) {
	if (expr && expr->return_type != type) {
		return make_unique<CastExpression>(type, move(expr));
	}
	return expr;
}

void LogicalPlanGenerator::Visit(AggregateExpression &expr) {
	SQLNodeVisitor::Visit(expr);
	// add cast to child of aggregate if types don't match
	expr.child = AddCastToType(expr.return_type, move(expr.child));
}

void LogicalPlanGenerator::Visit(CaseExpression &expr) {
	SQLNodeVisitor::Visit(expr);
	// check needs to be bool
	expr.check = AddCastToType(TypeId::BOOLEAN, move(expr.check));
	// res_if_true and res_if_false need the same type
	auto result_type =
	    GetMaxType({expr.return_type, expr.result_if_true->return_type, expr.result_if_false->return_type});
	expr.result_if_true = AddCastToType(result_type, move(expr.result_if_true));
	expr.result_if_false = AddCastToType(result_type, move(expr.result_if_false));
	assert(result_type == expr.return_type);
}

void LogicalPlanGenerator::Visit(ComparisonExpression &expr) {
	SQLNodeVisitor::Visit(expr);
	if (expr.left->return_type != expr.right->return_type) {
		// add a cast if the types don't match
		auto result_type = GetMaxType({expr.left->return_type, expr.right->return_type});
		expr.left = AddCastToType(result_type, move(expr.left));
		expr.right = AddCastToType(result_type, move(expr.right));
	}
}

void LogicalPlanGenerator::Visit(ConjunctionExpression &expr) {
	SQLNodeVisitor::Visit(expr);
	expr.left = AddCastToType(TypeId::BOOLEAN, move(expr.left));
	expr.right = AddCastToType(TypeId::BOOLEAN, move(expr.right));
}

void LogicalPlanGenerator::Visit(OperatorExpression &expr) {
	SQLNodeVisitor::Visit(expr);
	if (expr.type == ExpressionType::OPERATOR_NOT && expr.children[0]->return_type != TypeId::BOOLEAN) {
		expr.children[0] = AddCastToType(TypeId::BOOLEAN, move(expr.children[0]));
	} else {
		vector<TypeId> types = {expr.return_type};
		for (auto &child : expr.children) {
			types.push_back(child->return_type);
		}
		auto result_type = GetMaxType(types);
		for (size_t i = 0; i < expr.children.size(); i++) {
			expr.children[i] = AddCastToType(result_type, move(expr.children[i]));
		}
	}
}

unique_ptr<Expression> LogicalPlanGenerator::VisitReplace(BoundSubqueryExpression &expr, unique_ptr<Expression> *expr_ptr) {
	// first visit the children of the Subquery expression, if any
	VisitExpressionChildren(expr);

	// check if the subquery is correlated
	auto &subquery = (SubqueryExpression&) *expr.subquery;
	// first we translate the QueryNode of the subquery into a logical plan
	LogicalPlanGenerator generator(context, *expr.context);
	generator.CreatePlan(*subquery.subquery);
	if (!generator.root) {
		throw Exception("Can't plan subquery");
	}
	auto plan = move(generator.root);
	switch(subquery.subquery_type) {
	case SubqueryType::EXISTS: {
		if (expr.is_correlated) {
			throw Exception("Correlated exists not handled yet!");
		}
		// uncorrelated EXISTS
		// we only care about existence, hence we push a LIMIT 1 operator
		auto limit = make_unique<LogicalLimit>(1, 0);
		limit->AddChild(move(plan));
		plan = move(limit);

		// now we push a COUNT(*) aggregate onto the limit, this will be either 0 or 1 (EXISTS or NOT EXISTS)
		auto count_star = make_unique<AggregateExpression>(ExpressionType::AGGREGATE_COUNT_STAR, nullptr);
		count_star->ResolveType();
		auto count_type = count_star->return_type;
		vector<unique_ptr<Expression>> aggregate_list;
		aggregate_list.push_back(move(count_star));
		auto aggregate = make_unique<LogicalAggregate>(move(aggregate_list));
		aggregate->AddChild(move(plan));
		plan = move(aggregate);

		// now we push a projection with a comparison to 1
		auto left_child = make_unique<BoundExpression>(count_type, 0);
		auto right_child = make_unique<ConstantExpression>(Value::Numeric(count_type, 1));
		auto comparison = make_unique<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, move(left_child), move(right_child));

		vector<unique_ptr<Expression>> projection_list;
		projection_list.push_back(move(comparison));
		auto projection = make_unique<LogicalProjection>(move(projection_list));
		projection->AddChild(move(plan));
		plan = move(projection);

		// the projection gives us as only column the result of the EXISTS clause
		// now we push a LogicalSubquery node to give the projection a table index
		auto subquery_index = bind_context.GenerateTableIndex();
		auto subquery = make_unique<LogicalSubquery>(subquery_index, 1);
		subquery->AddChild(move(plan));
		plan = move(subquery);

		// we add it to the main query by adding a cross product
		// FIXME: should use something else besides cross product as we always add only one scalar constant
		if (root) {
			auto cross_product = make_unique<LogicalCrossProduct>();
			cross_product->AddChild(move(root));
			cross_product->AddChild(move(plan));
			root = move(cross_product);
		} else {
			root = move(plan);
		}

		// we replace the original subquery with a ColumnRefExpression refering to the result of the subquery (either TRUE or FALSE)
		return make_unique<BoundColumnRefExpression>(expr, TypeId::BOOLEAN, ColumnBinding(subquery_index, 0));
	}
	case SubqueryType::SCALAR: {
		if (!expr.is_correlated) {
			// in the uncorrelated case we are only interested in the first result of the query
			// hence we simply push a LIMIT 1 to get the first row of the subquery
			auto limit = make_unique<LogicalLimit>(1, 0);
			limit->AddChild(move(plan));
			plan = move(limit);
			// we push an aggregate that returns the FIRST element
			vector<unique_ptr<Expression>> expressions;
			auto bound = make_unique<BoundExpression>(expr.return_type, 0);
			auto first_agg = make_unique<AggregateExpression>(ExpressionType::AGGREGATE_FIRST, move(bound));
			first_agg->ResolveType();
			expressions.push_back(move(first_agg));
			auto aggr = make_unique<LogicalAggregate>(move(expressions));
			aggr->AddChild(move(plan));
			plan = move(aggr);
		}

		// now push a subquery op to get a table index to reference
		auto subquery_index = bind_context.GenerateTableIndex();
		auto logical_subquery = make_unique<LogicalSubquery>(subquery_index, 1);
		logical_subquery->AddChild(move(plan));
		plan = move(logical_subquery);

		if (!expr.is_correlated) {
			// in the uncorrelated case, we add the value to the main query through a cross product
			// FIXME: should use something else besides cross product as we always add only one scalar constant and cross product is not optimized for this. 
			assert(root);
			auto cross_product = make_unique<LogicalCrossProduct>();
			cross_product->AddChild(move(root));
			cross_product->AddChild(move(plan));
			root = move(cross_product);
		} else {
			// in the correlated case, we push a dependent join
			auto dependent_join = make_unique<LogicalJoin>(JoinType::DEPENDENT);
			dependent_join->AddChild(move(root));
			dependent_join->AddChild(move(plan));
			root = move(dependent_join);
		}
		// we replace the original subquery with a BoundColumnRefExpression refering to the scalar result of the subquery
		return make_unique<BoundColumnRefExpression>(expr, expr.return_type, ColumnBinding(subquery_index, 0));
	}
	case SubqueryType::ANY: {
		if (expr.is_correlated) {
			throw Exception("Correlated ANY not handled yet!");
		}
		// we generate a MARK join that results in either (TRUE, FALSE or NULL)
		// subquery has NULL values -> result is (TRUE or NULL)
		// subquery has no NULL values -> result is (TRUE, FALSE or NULL [if input is NULL])
		// first we push a subquery to the right hand side
		auto subquery_index = bind_context.GenerateTableIndex();
		auto logical_subquery = make_unique<LogicalSubquery>(subquery_index, 1);
		logical_subquery->AddChild(move(plan));
		plan = move(logical_subquery);

		// then we generate the MARK join with the subquery
		auto join = make_unique<LogicalJoin>(JoinType::MARK);
		join->AddChild(move(root));
		join->AddChild(move(plan));
		// create the JOIN condition
		JoinCondition cond;
		cond.left = move(subquery.child);
		cond.right = make_unique<BoundExpression>(cond.left->return_type, 0);
		cond.comparison = subquery.comparison_type;
		join->conditions.push_back(move(cond));
		root = move(join);

		// we replace the original subquery with a BoundColumnRefExpression refering to the mark column
		return make_unique<BoundColumnRefExpression>(expr, expr.return_type, ColumnBinding(subquery_index, 0));
	}
	default:
		assert(0);
		throw Exception("Unhandled subquery type!");

	}
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
	VisitExpression(&expr.condition);
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
	generator.CreatePlan(*expr.subquery);

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
