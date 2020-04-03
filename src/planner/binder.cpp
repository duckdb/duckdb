#include "duckdb/planner/binder.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parser/statement/list.hpp"
#include "duckdb/planner/bound_query_node.hpp"
#include "duckdb/planner/bound_tableref.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression_binder/constant_binder.hpp"

using namespace duckdb;
using namespace std;

Binder::Binder(ClientContext &context, Binder *parent_)
    : context(context), read_only(true), parent(!parent_ ? nullptr : (parent_->parent ? parent_->parent : parent_)),
      bound_tables(0) {
	if (parent_) {
		// We have to inherit CTE bindings from the parent bind_context, if there is a parent.
		bind_context.SetCTEBindings(parent_->bind_context.GetCTEBindings());
		bind_context.cte_references = parent_->bind_context.cte_references;
	}
	if (parent) {
		parameters = parent->parameters;
		CTE_bindings = parent->CTE_bindings;
	}
}

BoundStatement Binder::Bind(SQLStatement &statement) {
	switch (statement.type) {
	case StatementType::SELECT:
		return Bind((SelectStatement &)statement);
	case StatementType::INSERT:
		return Bind((InsertStatement &)statement);
	case StatementType::COPY:
		return Bind((CopyStatement &)statement);
	case StatementType::DELETE:
		return Bind((DeleteStatement &)statement);
	case StatementType::UPDATE:
		return Bind((UpdateStatement &)statement);
	case StatementType::RELATION:
		return Bind((RelationStatement &)statement);
	case StatementType::CREATE:
		return Bind((CreateStatement &)statement);
	case StatementType::DROP:
		return Bind((DropStatement &)statement);
	case StatementType::ALTER:
		return Bind((AlterTableStatement &)statement);
	case StatementType::TRANSACTION:
		return Bind((TransactionStatement &)statement);
	case StatementType::PRAGMA:
		return Bind((PragmaStatement &)statement);
	case StatementType::EXECUTE:
		return Bind((ExecuteStatement &)statement);
	case StatementType::EXPLAIN:
		return Bind((ExplainStatement &)statement);
	case StatementType::VACUUM:
		return Bind((VacuumStatement &)statement);
	default:
		throw NotImplementedException("Unimplemented statement type \"%s\" for Bind",
		                              StatementTypeToString(statement.type).c_str());
	}
}

static int64_t BindConstant(Binder &binder, ClientContext &context, string clause, unique_ptr<ParsedExpression> &expr) {
	ConstantBinder constant_binder(binder, context, clause);
	auto bound_expr = constant_binder.Bind(expr);
	Value value = ExpressionExecutor::EvaluateScalar(*bound_expr);
	if (!TypeIsNumeric(value.type)) {
		throw BinderException("LIMIT clause can only contain numeric constants!");
	}
	int64_t limit_value = value.GetValue<int64_t>();
	if (limit_value < 0) {
		throw BinderException("LIMIT must not be negative");
	}
	return limit_value;
}

unique_ptr<BoundQueryNode> Binder::BindNode(QueryNode &node) {
	unique_ptr<BoundQueryNode> result;
	switch (node.type) {
	case QueryNodeType::SELECT_NODE:
		result = BindNode((SelectNode &)node);
		break;
	case QueryNodeType::RECURSIVE_CTE_NODE:
		result = BindNode((RecursiveCTENode &)node);
		break;
	default:
		assert(node.type == QueryNodeType::SET_OPERATION_NODE);
		result = BindNode((SetOperationNode &)node);
		break;
	}
	// DISTINCT ON select list
	result->select_distinct = node.select_distinct;
	// bind the limit nodes
	if (node.limit) {
		result->limit = BindConstant(*this, context, "LIMIT clause", node.limit);
		result->offset = 0;
	}
	if (node.offset) {
		result->offset = BindConstant(*this, context, "OFFSET clause", node.offset);
		if (!node.limit) {
			result->limit = std::numeric_limits<int64_t>::max();
		}
	}
	return result;
}

BoundStatement Binder::Bind(QueryNode &node) {
	BoundStatement result;
	// bind the node
	auto bound_node = BindNode(node);

	result.names = bound_node->names;
	result.types = bound_node->types;

	// and plan it
	result.plan = CreatePlan(*bound_node);
	return result;
}

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundQueryNode &node) {
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
unique_ptr<BoundTableRef> Binder::Bind(TableRef &ref) {
	switch (ref.type) {
	case TableReferenceType::BASE_TABLE:
		return Bind((BaseTableRef &)ref);
	case TableReferenceType::CROSS_PRODUCT:
		return Bind((CrossProductRef &)ref);
	case TableReferenceType::JOIN:
		return Bind((JoinRef &)ref);
	case TableReferenceType::SUBQUERY:
		return Bind((SubqueryRef &)ref);
	case TableReferenceType::EMPTY:
		return Bind((EmptyTableRef &)ref);
	case TableReferenceType::TABLE_FUNCTION:
		return Bind((TableFunctionRef &)ref);
	case TableReferenceType::EXPRESSION_LIST:
		return Bind((ExpressionListRef &)ref);
	default:
		throw Exception("Unknown table ref type");
	}
}

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundTableRef &ref) {
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

void Binder::AddCTE(const string &name, QueryNode *cte) {
	assert(cte);
	assert(!name.empty());
	auto entry = CTE_bindings.find(name);
	if (entry != CTE_bindings.end()) {
		throw BinderException("Duplicate CTE \"%s\" in query!", name.c_str());
	}
	CTE_bindings[name] = cte;
}

unique_ptr<QueryNode> Binder::FindCTE(const string &name) {
	auto entry = CTE_bindings.find(name);
	if (entry == CTE_bindings.end()) {
		if (parent) {
			return parent->FindCTE(name);
		}
		return nullptr;
	}
	return entry->second->Copy();
}

idx_t Binder::GenerateTableIndex() {
	if (parent) {
		return parent->GenerateTableIndex();
	}
	return bound_tables++;
}

void Binder::PushExpressionBinder(ExpressionBinder *binder) {
	GetActiveBinders().push_back(binder);
}

void Binder::PopExpressionBinder() {
	assert(HasActiveBinder());
	GetActiveBinders().pop_back();
}

void Binder::SetActiveBinder(ExpressionBinder *binder) {
	assert(HasActiveBinder());
	GetActiveBinders().back() = binder;
}

ExpressionBinder *Binder::GetActiveBinder() {
	return GetActiveBinders().back();
}

bool Binder::HasActiveBinder() {
	return GetActiveBinders().size() > 0;
}

vector<ExpressionBinder *> &Binder::GetActiveBinders() {
	if (parent) {
		return parent->GetActiveBinders();
	}
	return active_binders;
}

void Binder::MoveCorrelatedExpressions(Binder &other) {
	MergeCorrelatedColumns(other.correlated_columns);
	other.correlated_columns.clear();
}

void Binder::MergeCorrelatedColumns(vector<CorrelatedColumnInfo> &other) {
	for (idx_t i = 0; i < other.size(); i++) {
		AddCorrelatedColumn(other[i]);
	}
}

void Binder::AddCorrelatedColumn(CorrelatedColumnInfo info) {
	// we only add correlated columns to the list if they are not already there
	if (std::find(correlated_columns.begin(), correlated_columns.end(), info) == correlated_columns.end()) {
		correlated_columns.push_back(info);
	}
}
