#include "duckdb/planner/logical_plan_generator.hpp"

#include "duckdb/planner/bound_query_node.hpp"
#include "duckdb/planner/bound_sql_statement.hpp"
#include "duckdb/planner/bound_tableref.hpp"

using namespace duckdb;
using namespace std;

LogicalPlanGenerator::LogicalPlanGenerator(Binder &binder, ClientContext &context)
    : plan_subquery(true), has_unplanned_subqueries(false), binder(binder), require_row_id(false), context(context) {
}

unique_ptr<LogicalOperator> LogicalPlanGenerator::CreatePlan(BoundSQLStatement &statement) {
	switch (statement.type) {
	case StatementType::SELECT:
		return CreatePlan((BoundSelectStatement &)statement);
	case StatementType::INSERT:
		return CreatePlan((BoundInsertStatement &)statement);
	case StatementType::COPY:
		return CreatePlan((BoundCopyStatement &)statement);
	case StatementType::DELETE:
		return CreatePlan((BoundDeleteStatement &)statement);
	case StatementType::UPDATE:
		return CreatePlan((BoundUpdateStatement &)statement);
	case StatementType::CREATE:
		return CreatePlan((BoundCreateStatement &)statement);
	case StatementType::DROP:
	case StatementType::ALTER:
	case StatementType::TRANSACTION:
	case StatementType::PRAGMA:
		return CreatePlan((BoundSimpleStatement &)statement);
	case StatementType::EXECUTE:
		return CreatePlan((BoundExecuteStatement &)statement);
	case StatementType::EXPLAIN:
		return CreatePlan((BoundExplainStatement &)statement);
	default:
		throw Exception("Unsupported bound statement type");
	}
}

unique_ptr<LogicalOperator> LogicalPlanGenerator::CreatePlan(BoundQueryNode &node) {
	switch (node.type) {
	case QueryNodeType::SELECT_NODE:
		return CreatePlan((BoundSelectNode &)node);
	case QueryNodeType::SET_OPERATION_NODE:
		return CreatePlan((BoundSetOperationNode &)node);
    case QueryNodeType::RECURSIVE_CTE_NODE:
        return CreatePlan((BoundRecursiveCTENode &)node);
	default:
		throw Exception("Unsupported bound query node type");
	}
}

unique_ptr<LogicalOperator> LogicalPlanGenerator::CreatePlan(BoundTableRef &ref) {
	switch (ref.type) {
	case TableReferenceType::BASE_TABLE:
		return CreatePlan((BoundBaseTableRef &)ref);
	case TableReferenceType::SUBQUERY:
		return CreatePlan((BoundSubqueryRef &)ref);
	case TableReferenceType::JOIN:
		return CreatePlan((BoundJoinRef &)ref);
	case TableReferenceType::CROSS_PRODUCT:
		return CreatePlan((BoundCrossProductRef &)ref);
	case TableReferenceType::TABLE_FUNCTION:
		return CreatePlan((BoundTableFunction &)ref);
	case TableReferenceType::EMPTY:
		return CreatePlan((BoundEmptyTableRef &)ref);
	case TableReferenceType::EXPRESSION_LIST:
		return CreatePlan((BoundExpressionListRef &)ref);
    case TableReferenceType::CTE:
        return CreatePlan((BoundCTERef &)ref);
	default:
		throw Exception("Unsupported bound table ref type type");
	}
}
