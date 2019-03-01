#include "planner/logical_plan_generator.hpp"

#include "execution/expression_executor.hpp"
#include "main/client_context.hpp"
#include "main/database.hpp"
#include "parser/expression/list.hpp"
#include "parser/query_node/list.hpp"
#include "parser/statement/list.hpp"
#include "parser/tableref/list.hpp"
#include "planner/binder.hpp"
#include "planner/operator/list.hpp"

#include <map>

using namespace duckdb;
using namespace std;

LogicalPlanGenerator::LogicalPlanGenerator(Binder &binder, ClientContext &context, bool allow_parameter)
    : binder(binder), plan_subquery(true), has_unplanned_subqueries(false), allow_parameter(allow_parameter),
      require_row_id(false), context(context) {
}

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
	case StatementType::EXECUTE:
		CreatePlan((ExecuteStatement &)statement);
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

unique_ptr<TableRef> LogicalPlanGenerator::Visit(BaseTableRef &expr) {
	// FIXME: catalog access should only happen once in binder
	auto table = context.db.catalog.GetTable(context.ActiveTransaction(), expr.schema_name, expr.table_name);
	auto alias = expr.alias.empty() ? expr.table_name : expr.alias;

	auto index = binder.bind_context.GetBindingIndex(alias);

	vector<column_t> column_ids;
	// look in the context for this table which columns are required
	for (auto &bound_column : binder.bind_context.bound_columns[alias]) {
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

	AcceptChild(&expr.right);
	assert(root);
	cross_product->AddChild(move(root));

	root = move(cross_product);
	return nullptr;
}

unique_ptr<TableRef> LogicalPlanGenerator::Visit(SubqueryRef &expr) {
	// generate the logical plan for the subquery
	// this happens separately from the current LogicalPlan generation
	LogicalPlanGenerator generator(*expr.binder, context);

	size_t column_count = expr.subquery->GetSelectList().size();
	generator.CreatePlan(*expr.subquery);

	auto index = binder.bind_context.GetBindingIndex(expr.alias);

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

	auto index =
	    binder.bind_context.GetBindingIndex(expr.alias.empty() ? function_definition->function_name : expr.alias);

	if (root) {
		throw Exception("Table function cannot have children");
	}
	root = make_unique<LogicalTableFunction>(function, index, move(expr.function));
	return nullptr;
}

unique_ptr<Expression> LogicalPlanGenerator::VisitReplace(OperatorExpression &expr, unique_ptr<Expression> *expr_ptr) {
	if (expr.type != ExpressionType::COMPARE_IN) {
		return nullptr;
	}
	if (expr.children[0]->IsScalar()) {
		// LHS is scalar: we can flatten the entire list
		return nullptr;
	}
	if (expr.children.size() < 6) {
		// not enough children for flattening to be worth it
		return nullptr;
	}
	assert(root);
	auto in_type = expr.children[0]->return_type;
	// IN clause with many children: try to generate a mark join that replaces this IN expression
	// we can only do this if the expressions in the expression list are scalar
	for (size_t i = 1; i < expr.children.size(); i++) {
		assert(expr.children[i]->return_type == in_type);
		if (!expr.children[i]->IsScalar()) {
			// non-scalar expression
			return nullptr;
		}
	}
	// IN clause with many constant children
	// generate a mark join that replaces this IN expression
	// first generate a ChunkCollection from the set of expressions
	vector<TypeId> types = {in_type};
	auto collection = make_unique<ChunkCollection>();
	DataChunk chunk;
	chunk.Initialize(types);
	for (size_t i = 1; i < expr.children.size(); i++) {
		// reoslve this expression to a constant
		auto value = ExpressionExecutor::EvaluateScalar(*expr.children[i]);
		size_t index = chunk.data[0].count++;
		chunk.data[0].SetValue(index, value);
		if (chunk.data[0].count == STANDARD_VECTOR_SIZE || i + 1 == expr.children.size()) {
			// chunk full: append to chunk collection
			collection->Append(chunk);
			chunk.Reset();
		}
	}
	// now generate a ChunkGet that scans this collection
	auto chunk_index = binder.GenerateTableIndex();
	auto chunk_scan = make_unique<LogicalChunkGet>(chunk_index, types, move(collection));

	auto subquery_index = binder.GenerateTableIndex();
	auto logical_subquery = make_unique<LogicalSubquery>(subquery_index, 1);
	logical_subquery->AddChild(move(chunk_scan));

	// then we generate the MARK join with the chunk scan on the RHS
	auto join = make_unique<LogicalComparisonJoin>(JoinType::MARK);
	join->AddChild(move(root));
	join->AddChild(move(logical_subquery));
	// create the JOIN condition
	JoinCondition cond;
	cond.left = move(expr.children[0]);
	cond.right = make_unique<BoundColumnRefExpression>("", in_type, ColumnBinding(subquery_index, 0));
	cond.comparison = ExpressionType::COMPARE_EQUAL;
	join->conditions.push_back(move(cond));
	root = move(join);

	// we replace the original subquery with a BoundColumnRefExpression refering to the mark column
	return make_unique<BoundColumnRefExpression>(expr, TypeId::BOOLEAN, ColumnBinding(subquery_index, 0));
}
