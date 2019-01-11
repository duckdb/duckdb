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
	default:
		assert(statement.type == StatementType::CREATE_INDEX);
		CreatePlan((CreateIndexStatement &)statement);
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

void LogicalPlanGenerator::Visit(SubqueryExpression &expr) {
	LogicalPlanGenerator generator(context, *expr.context);
	generator.CreatePlan(*expr.subquery);
	if (!generator.root) {
		throw Exception("Can't plan subquery");
	}
	expr.op = move(generator.root);
	expr.subquery = nullptr;
	assert(expr.op);
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
