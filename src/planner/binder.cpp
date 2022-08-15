#include "duckdb/planner/binder.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/list.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/bound_query_node.hpp"
#include "duckdb/planner/bound_tableref.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression_binder/returning_binder.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_sample.hpp"

#include <algorithm>

namespace duckdb {

shared_ptr<Binder> Binder::CreateBinder(ClientContext &context, Binder *parent, bool inherit_ctes) {
	return make_shared<Binder>(true, context, parent ? parent->shared_from_this() : nullptr, inherit_ctes);
}

Binder::Binder(bool, ClientContext &context, shared_ptr<Binder> parent_p, bool inherit_ctes_p)
    : context(context), parent(move(parent_p)), bound_tables(0), inherit_ctes(inherit_ctes_p) {
	parameters = nullptr;
	if (parent) {

		// We have to inherit macro and lambda parameter bindings and from the parent binder, if there is a parent.
		macro_binding = parent->macro_binding;
		lambda_bindings = parent->lambda_bindings;

		if (inherit_ctes) {
			// We have to inherit CTE bindings from the parent bind_context, if there is a parent.
			bind_context.SetCTEBindings(parent->bind_context.GetCTEBindings());
			bind_context.cte_references = parent->bind_context.cte_references;
			parameters = parent->parameters;
		}
	}
}

BoundStatement Binder::Bind(SQLStatement &statement) {
	root_statement = &statement;
	switch (statement.type) {
	case StatementType::SELECT_STATEMENT:
		return Bind((SelectStatement &)statement);
	case StatementType::INSERT_STATEMENT:
		return Bind((InsertStatement &)statement);
	case StatementType::COPY_STATEMENT:
		return Bind((CopyStatement &)statement);
	case StatementType::DELETE_STATEMENT:
		return Bind((DeleteStatement &)statement);
	case StatementType::UPDATE_STATEMENT:
		return Bind((UpdateStatement &)statement);
	case StatementType::RELATION_STATEMENT:
		return Bind((RelationStatement &)statement);
	case StatementType::CREATE_STATEMENT:
		return Bind((CreateStatement &)statement);
	case StatementType::DROP_STATEMENT:
		return Bind((DropStatement &)statement);
	case StatementType::ALTER_STATEMENT:
		return Bind((AlterStatement &)statement);
	case StatementType::TRANSACTION_STATEMENT:
		return Bind((TransactionStatement &)statement);
	case StatementType::PRAGMA_STATEMENT:
		return Bind((PragmaStatement &)statement);
	case StatementType::EXPLAIN_STATEMENT:
		return Bind((ExplainStatement &)statement);
	case StatementType::VACUUM_STATEMENT:
		return Bind((VacuumStatement &)statement);
	case StatementType::SHOW_STATEMENT:
		return Bind((ShowStatement &)statement);
	case StatementType::CALL_STATEMENT:
		return Bind((CallStatement &)statement);
	case StatementType::EXPORT_STATEMENT:
		return Bind((ExportStatement &)statement);
	case StatementType::SET_STATEMENT:
		return Bind((SetStatement &)statement);
	case StatementType::LOAD_STATEMENT:
		return Bind((LoadStatement &)statement);
	case StatementType::EXTENSION_STATEMENT:
		return Bind((ExtensionStatement &)statement);
	case StatementType::PREPARE_STATEMENT:
		return Bind((PrepareStatement &)statement);
	case StatementType::EXECUTE_STATEMENT:
		return Bind((ExecuteStatement &)statement);
	default: // LCOV_EXCL_START
		throw NotImplementedException("Unimplemented statement type \"%s\" for Bind",
		                              StatementTypeToString(statement.type));
	} // LCOV_EXCL_STOP
}

void Binder::AddCTEMap(CommonTableExpressionMap &cte_map) {
	for (auto &cte_it : cte_map.map) {
		AddCTE(cte_it.first, cte_it.second.get());
	}
}

unique_ptr<BoundQueryNode> Binder::BindNode(QueryNode &node) {
	// first we visit the set of CTEs and add them to the bind context
	AddCTEMap(node.cte_map);
	// now we bind the node
	unique_ptr<BoundQueryNode> result;
	switch (node.type) {
	case QueryNodeType::SELECT_NODE:
		result = BindNode((SelectNode &)node);
		break;
	case QueryNodeType::RECURSIVE_CTE_NODE:
		result = BindNode((RecursiveCTENode &)node);
		break;
	default:
		D_ASSERT(node.type == QueryNodeType::SET_OPERATION_NODE);
		result = BindNode((SetOperationNode &)node);
		break;
	}
	return result;
}

BoundStatement Binder::Bind(QueryNode &node) {
	auto bound_node = BindNode(node);

	BoundStatement result;
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
		throw InternalException("Unsupported bound query node type");
	}
}

unique_ptr<BoundTableRef> Binder::Bind(TableRef &ref) {
	unique_ptr<BoundTableRef> result;
	switch (ref.type) {
	case TableReferenceType::BASE_TABLE:
		result = Bind((BaseTableRef &)ref);
		break;
	case TableReferenceType::CROSS_PRODUCT:
		result = Bind((CrossProductRef &)ref);
		break;
	case TableReferenceType::JOIN:
		result = Bind((JoinRef &)ref);
		break;
	case TableReferenceType::SUBQUERY:
		result = Bind((SubqueryRef &)ref);
		break;
	case TableReferenceType::EMPTY:
		result = Bind((EmptyTableRef &)ref);
		break;
	case TableReferenceType::TABLE_FUNCTION:
		result = Bind((TableFunctionRef &)ref);
		break;
	case TableReferenceType::EXPRESSION_LIST:
		result = Bind((ExpressionListRef &)ref);
		break;
	default:
		throw InternalException("Unknown table ref type");
	}
	result->sample = move(ref.sample);
	return result;
}

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundTableRef &ref) {
	unique_ptr<LogicalOperator> root;
	switch (ref.type) {
	case TableReferenceType::BASE_TABLE:
		root = CreatePlan((BoundBaseTableRef &)ref);
		break;
	case TableReferenceType::SUBQUERY:
		root = CreatePlan((BoundSubqueryRef &)ref);
		break;
	case TableReferenceType::JOIN:
		root = CreatePlan((BoundJoinRef &)ref);
		break;
	case TableReferenceType::CROSS_PRODUCT:
		root = CreatePlan((BoundCrossProductRef &)ref);
		break;
	case TableReferenceType::TABLE_FUNCTION:
		root = CreatePlan((BoundTableFunction &)ref);
		break;
	case TableReferenceType::EMPTY:
		root = CreatePlan((BoundEmptyTableRef &)ref);
		break;
	case TableReferenceType::EXPRESSION_LIST:
		root = CreatePlan((BoundExpressionListRef &)ref);
		break;
	case TableReferenceType::CTE:
		root = CreatePlan((BoundCTERef &)ref);
		break;
	default:
		throw InternalException("Unsupported bound table ref type type");
	}
	// plan the sample clause
	if (ref.sample) {
		root = make_unique<LogicalSample>(move(ref.sample), move(root));
	}
	return root;
}

void Binder::AddCTE(const string &name, CommonTableExpressionInfo *info) {
	D_ASSERT(info);
	D_ASSERT(!name.empty());
	auto entry = CTE_bindings.find(name);
	if (entry != CTE_bindings.end()) {
		throw InternalException("Duplicate CTE \"%s\" in query!", name);
	}
	CTE_bindings[name] = info;
}

CommonTableExpressionInfo *Binder::FindCTE(const string &name, bool skip) {
	auto entry = CTE_bindings.find(name);
	if (entry != CTE_bindings.end()) {
		if (!skip || entry->second->query->node->type == QueryNodeType::RECURSIVE_CTE_NODE) {
			return entry->second;
		}
	}
	if (parent && inherit_ctes) {
		return parent->FindCTE(name, name == alias);
	}
	return nullptr;
}

bool Binder::CTEIsAlreadyBound(CommonTableExpressionInfo *cte) {
	if (bound_ctes.find(cte) != bound_ctes.end()) {
		return true;
	}
	if (parent && inherit_ctes) {
		return parent->CTEIsAlreadyBound(cte);
	}
	return false;
}

void Binder::AddBoundView(ViewCatalogEntry *view) {
	// check if the view is already bound
	auto current = this;
	while (current) {
		if (current->bound_views.find(view) != current->bound_views.end()) {
			throw BinderException("infinite recursion detected: attempting to recursively bind view \"%s\"",
			                      view->name);
		}
		current = current->parent.get();
	}
	bound_views.insert(view);
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
	D_ASSERT(HasActiveBinder());
	GetActiveBinders().pop_back();
}

void Binder::SetActiveBinder(ExpressionBinder *binder) {
	D_ASSERT(HasActiveBinder());
	GetActiveBinders().back() = binder;
}

ExpressionBinder *Binder::GetActiveBinder() {
	return GetActiveBinders().back();
}

bool Binder::HasActiveBinder() {
	return !GetActiveBinders().empty();
}

vector<ExpressionBinder *> &Binder::GetActiveBinders() {
	if (parent) {
		return parent->GetActiveBinders();
	}
	return active_binders;
}

void Binder::AddUsingBindingSet(unique_ptr<UsingColumnSet> set) {
	if (parent) {
		parent->AddUsingBindingSet(move(set));
		return;
	}
	bind_context.AddUsingBindingSet(move(set));
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

void Binder::AddCorrelatedColumn(const CorrelatedColumnInfo &info) {
	// we only add correlated columns to the list if they are not already there
	if (std::find(correlated_columns.begin(), correlated_columns.end(), info) == correlated_columns.end()) {
		correlated_columns.push_back(info);
	}
}

bool Binder::HasMatchingBinding(const string &table_name, const string &column_name, string &error_message) {
	string empty_schema;
	return HasMatchingBinding(empty_schema, table_name, column_name, error_message);
}

bool Binder::HasMatchingBinding(const string &schema_name, const string &table_name, const string &column_name,
                                string &error_message) {
	Binding *binding = nullptr;
	D_ASSERT(!lambda_bindings);
	if (macro_binding && table_name == macro_binding->alias) {
		binding = macro_binding;
	} else {
		binding = bind_context.GetBinding(table_name, error_message);
	}

	if (!binding) {
		return false;
	}
	if (!schema_name.empty()) {
		auto catalog_entry = binding->GetStandardEntry();
		if (!catalog_entry) {
			return false;
		}
		if (catalog_entry->schema->name != schema_name || catalog_entry->name != table_name) {
			return false;
		}
	}
	bool binding_found;
	binding_found = binding->HasMatchingBinding(column_name);
	if (!binding_found) {
		error_message = binding->ColumnNotFoundError(column_name);
	}
	return binding_found;
}

void Binder::SetBindingMode(BindingMode mode) {
	if (parent) {
		parent->SetBindingMode(mode);
	}
	this->mode = mode;
}

BindingMode Binder::GetBindingMode() {
	if (parent) {
		return parent->GetBindingMode();
	}
	return mode;
}

void Binder::SetCanContainNulls(bool can_contain_nulls_p) {
	can_contain_nulls = can_contain_nulls_p;
}

void Binder::AddTableName(string table_name) {
	if (parent) {
		parent->AddTableName(move(table_name));
		return;
	}
	table_names.insert(move(table_name));
}

const unordered_set<string> &Binder::GetTableNames() {
	if (parent) {
		return parent->GetTableNames();
	}
	return table_names;
}

string Binder::FormatError(ParsedExpression &expr_context, const string &message) {
	return FormatError(expr_context.query_location, message);
}

string Binder::FormatError(TableRef &ref_context, const string &message) {
	return FormatError(ref_context.query_location, message);
}

string Binder::FormatErrorRecursive(idx_t query_location, const string &message, vector<ExceptionFormatValue> &values) {
	QueryErrorContext context(root_statement, query_location);
	return context.FormatErrorRecursive(message, values);
}

BoundStatement Binder::BindReturning(vector<unique_ptr<ParsedExpression>> returning_list, TableCatalogEntry *table,
                                     idx_t update_table_index, unique_ptr<LogicalOperator> child_operator,
                                     BoundStatement result) {

	vector<LogicalType> types;
	vector<std::string> names;

	auto binder = Binder::CreateBinder(context);

	for (auto &col : table->columns) {
		names.push_back(col.Name());
		types.push_back(col.Type());
	}

	binder->bind_context.AddGenericBinding(update_table_index, table->name, names, types);
	ReturningBinder returning_binder(*binder, context);

	vector<unique_ptr<Expression>> projection_expressions;
	LogicalType result_type;
	for (auto &returning_expr : returning_list) {
		auto expr_type = returning_expr->GetExpressionType();
		if (expr_type == ExpressionType::STAR) {
			auto generated_star_list = vector<unique_ptr<ParsedExpression>>();
			binder->bind_context.GenerateAllColumnExpressions((StarExpression &)*returning_expr, generated_star_list);

			for (auto &star_column : generated_star_list) {
				auto star_expr = returning_binder.Bind(star_column, &result_type);
				result.types.push_back(result_type);
				result.names.push_back(star_expr->GetName());
				projection_expressions.push_back(move(star_expr));
			}
		} else {
			auto expr = returning_binder.Bind(returning_expr, &result_type);
			result.names.push_back(expr->GetName());
			result.types.push_back(result_type);
			projection_expressions.push_back(move(expr));
		}
	}

	auto projection = make_unique<LogicalProjection>(GenerateTableIndex(), move(projection_expressions));
	projection->AddChild(move(child_operator));
	D_ASSERT(result.types.size() == result.names.size());
	result.plan = move(projection);
	properties.allow_stream_result = true;
	properties.return_type = StatementReturnType::QUERY_RESULT;
	return result;
}

} // namespace duckdb
