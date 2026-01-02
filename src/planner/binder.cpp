#include "duckdb/planner/binder.hpp"

#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/query_node/list.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/list.hpp"
#include "duckdb/parser/tableref/list.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/bound_query_node.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression_binder/returning_binder.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_sample.hpp"
#include "duckdb/planner/query_node/list.hpp"
#include "duckdb/planner/tableref/list.hpp"

#include <algorithm>

namespace duckdb {

idx_t Binder::GetBinderDepth() const {
	return depth;
}

void Binder::IncreaseDepth() {
	depth++;
	if (depth > context.config.max_expression_depth) {
		throw BinderException("Max expression depth limit of %lld exceeded. Use \"SET max_expression_depth TO x\" to "
		                      "increase the maximum expression depth.",
		                      context.config.max_expression_depth);
	}
}

shared_ptr<Binder> Binder::CreateBinder(ClientContext &context, optional_ptr<Binder> parent, BinderType binder_type) {
	return shared_ptr<Binder>(new Binder(context, parent ? parent->shared_from_this() : nullptr, binder_type));
}

Binder::Binder(ClientContext &context, shared_ptr<Binder> parent_p, BinderType binder_type)
    : context(context), bind_context(*this), parent(std::move(parent_p)), binder_type(binder_type),
      global_binder_state(parent ? parent->global_binder_state : make_shared_ptr<GlobalBinderState>()),
      query_binder_state(parent && binder_type == BinderType::REGULAR_BINDER ? parent->query_binder_state
                                                                             : make_shared_ptr<QueryBinderState>()),
      entry_retriever(context), depth(parent ? parent->GetBinderDepth() : 1) {
	IncreaseDepth();
	if (parent) {
		entry_retriever.Inherit(parent->entry_retriever);

		// We have to inherit macro and lambda parameter bindings and from the parent binder, if there is a parent.
		macro_binding = parent->macro_binding;
		lambda_bindings = parent->lambda_bindings;
	}
}

template <class T>
BoundStatement Binder::BindWithCTE(T &statement) {
	auto &cte_map = statement.cte_map;
	if (cte_map.map.empty()) {
		return Bind(statement);
	}

	auto stmt_node = make_uniq<StatementNode>(statement);
	stmt_node->cte_map = cte_map.Copy();
	return Bind(*stmt_node);
}

BoundStatement Binder::Bind(SQLStatement &statement) {
	switch (statement.type) {
	case StatementType::SELECT_STATEMENT:
		return Bind(statement.Cast<SelectStatement>());
	case StatementType::INSERT_STATEMENT:
		return BindWithCTE(statement.Cast<InsertStatement>());
	case StatementType::COPY_STATEMENT:
		return Bind(statement.Cast<CopyStatement>(), CopyToType::COPY_TO_FILE);
	case StatementType::DELETE_STATEMENT:
		return BindWithCTE(statement.Cast<DeleteStatement>());
	case StatementType::UPDATE_STATEMENT:
		return BindWithCTE(statement.Cast<UpdateStatement>());
	case StatementType::RELATION_STATEMENT:
		return Bind(statement.Cast<RelationStatement>());
	case StatementType::CREATE_STATEMENT:
		return Bind(statement.Cast<CreateStatement>());
	case StatementType::DROP_STATEMENT:
		return Bind(statement.Cast<DropStatement>());
	case StatementType::ALTER_STATEMENT:
		return Bind(statement.Cast<AlterStatement>());
	case StatementType::TRANSACTION_STATEMENT:
		return Bind(statement.Cast<TransactionStatement>());
	case StatementType::PRAGMA_STATEMENT:
		return Bind(statement.Cast<PragmaStatement>());
	case StatementType::EXPLAIN_STATEMENT:
		return Bind(statement.Cast<ExplainStatement>());
	case StatementType::VACUUM_STATEMENT:
		return Bind(statement.Cast<VacuumStatement>());
	case StatementType::CALL_STATEMENT:
		return Bind(statement.Cast<CallStatement>());
	case StatementType::EXPORT_STATEMENT:
		return Bind(statement.Cast<ExportStatement>());
	case StatementType::SET_STATEMENT:
		return Bind(statement.Cast<SetStatement>());
	case StatementType::LOAD_STATEMENT:
		return Bind(statement.Cast<LoadStatement>());
	case StatementType::EXTENSION_STATEMENT:
		return Bind(statement.Cast<ExtensionStatement>());
	case StatementType::PREPARE_STATEMENT:
		return Bind(statement.Cast<PrepareStatement>());
	case StatementType::EXECUTE_STATEMENT:
		return Bind(statement.Cast<ExecuteStatement>());
	case StatementType::LOGICAL_PLAN_STATEMENT:
		return Bind(statement.Cast<LogicalPlanStatement>());
	case StatementType::ATTACH_STATEMENT:
		return Bind(statement.Cast<AttachStatement>());
	case StatementType::DETACH_STATEMENT:
		return Bind(statement.Cast<DetachStatement>());
	case StatementType::COPY_DATABASE_STATEMENT:
		return Bind(statement.Cast<CopyDatabaseStatement>());
	case StatementType::UPDATE_EXTENSIONS_STATEMENT:
		return Bind(statement.Cast<UpdateExtensionsStatement>());
	case StatementType::MERGE_INTO_STATEMENT:
		return BindWithCTE(statement.Cast<MergeIntoStatement>());
	default: // LCOV_EXCL_START
		throw NotImplementedException("Unimplemented statement type \"%s\" for Bind",
		                              StatementTypeToString(statement.type));
	} // LCOV_EXCL_STOP
}

BoundStatement Binder::Bind(QueryNode &node) {
	return BindNode(node);
}

BoundStatement Binder::Bind(TableRef &ref) {
	BoundStatement result;
	switch (ref.type) {
	case TableReferenceType::BASE_TABLE:
		result = Bind(ref.Cast<BaseTableRef>());
		break;
	case TableReferenceType::JOIN:
		result = Bind(ref.Cast<JoinRef>());
		break;
	case TableReferenceType::SUBQUERY:
		result = Bind(ref.Cast<SubqueryRef>());
		break;
	case TableReferenceType::EMPTY_FROM:
		result = Bind(ref.Cast<EmptyTableRef>());
		break;
	case TableReferenceType::TABLE_FUNCTION:
		result = Bind(ref.Cast<TableFunctionRef>());
		break;
	case TableReferenceType::EXPRESSION_LIST:
		result = Bind(ref.Cast<ExpressionListRef>());
		break;
	case TableReferenceType::COLUMN_DATA:
		result = Bind(ref.Cast<ColumnDataRef>());
		break;
	case TableReferenceType::PIVOT:
		result = Bind(ref.Cast<PivotRef>());
		break;
	case TableReferenceType::SHOW_REF:
		result = Bind(ref.Cast<ShowRef>());
		break;
	case TableReferenceType::DELIM_GET:
		result = Bind(ref.Cast<DelimGetRef>());
		break;
	case TableReferenceType::BOUND_TABLE_REF:
		result = Bind(ref.Cast<BoundRefWrapper>());
		break;
	case TableReferenceType::CTE:
	case TableReferenceType::INVALID:
	default:
		throw InternalException("Unknown table ref type (%s)", EnumUtil::ToString(ref.type));
	}
	if (ref.sample) {
		result.plan = make_uniq<LogicalSample>(std::move(ref.sample), std::move(result.plan));
	}
	return result;
}

optional_ptr<CTEBinding> Binder::GetCTEBinding(const BindingAlias &name) {
	reference<Binder> current_binder(*this);
	optional_ptr<CTEBinding> result;
	while (true) {
		auto &current = current_binder.get();
		auto entry = current.bind_context.GetCTEBinding(name);
		if (entry) {
			// we only directly return the CTE if it can be referenced
			// if it cannot be referenced (circular reference) we keep going up the stack
			// to look for a CTE that can be referenced
			if (entry->CanBeReferenced()) {
				return entry;
			}
			result = entry;
		}
		if (!current.parent || current.binder_type != BinderType::REGULAR_BINDER) {
			break;
		}
		current_binder = *current.parent;
	}
	return result;
}

void Binder::AddBoundView(ViewCatalogEntry &view) {
	// check if the view is already bound
	auto current = this;
	while (current) {
		if (current->bound_views.find(view) != current->bound_views.end()) {
			throw BinderException("infinite recursion detected: attempting to recursively bind view \"%s\"", view.name);
		}
		current = current->parent.get();
	}
	bound_views.insert(view);
}

idx_t Binder::GenerateTableIndex() {
	return global_binder_state->bound_tables++;
}

StatementProperties &Binder::GetStatementProperties() {
	return global_binder_state->prop;
}

optional_ptr<BoundParameterMap> Binder::GetParameters() {
	return global_binder_state->parameters;
}

void Binder::SetParameters(BoundParameterMap &parameters) {
	global_binder_state->parameters = parameters;
}

void Binder::PushExpressionBinder(ExpressionBinder &binder) {
	GetActiveBinders().push_back(binder);
}

void Binder::PopExpressionBinder() {
	D_ASSERT(HasActiveBinder());
	GetActiveBinders().pop_back();
}

void Binder::SetActiveBinder(ExpressionBinder &binder) {
	D_ASSERT(HasActiveBinder());
	GetActiveBinders().back() = binder;
}

ExpressionBinder &Binder::GetActiveBinder() {
	return GetActiveBinders().back();
}

bool Binder::HasActiveBinder() {
	return !GetActiveBinders().empty();
}

vector<reference<ExpressionBinder>> &Binder::GetActiveBinders() {
	return query_binder_state->active_binders;
}

void Binder::AddUsingBindingSet(unique_ptr<UsingColumnSet> set) {
	global_binder_state->using_column_sets.push_back(std::move(set));
}

void Binder::MoveCorrelatedExpressions(Binder &other) {
	MergeCorrelatedColumns(other.correlated_columns);
	other.correlated_columns.clear();
}

void Binder::MergeCorrelatedColumns(CorrelatedColumns &other) {
	for (idx_t i = 0; i < other.size(); i++) {
		AddCorrelatedColumn(other[i]);
	}
}

void Binder::AddCorrelatedColumn(const CorrelatedColumnInfo &info) {
	// we only add correlated columns to the list if they are not already there
	if (std::find(correlated_columns.begin(), correlated_columns.end(), info) == correlated_columns.end()) {
		correlated_columns.AddColumn(info);
	}
}

optional_ptr<Binding> Binder::GetMatchingBinding(const string &table_name, const string &column_name,
                                                 ErrorData &error) {
	string empty_schema;
	return GetMatchingBinding(empty_schema, table_name, column_name, error);
}

optional_ptr<Binding> Binder::GetMatchingBinding(const string &schema_name, const string &table_name,
                                                 const string &column_name, ErrorData &error) {
	string empty_catalog;
	return GetMatchingBinding(empty_catalog, schema_name, table_name, column_name, error);
}

optional_ptr<Binding> Binder::GetMatchingBinding(const string &catalog_name, const string &schema_name,
                                                 const string &table_name, const string &column_name,
                                                 ErrorData &error) {
	optional_ptr<Binding> binding;
	if (macro_binding && table_name == macro_binding->GetAlias()) {
		binding = optional_ptr<Binding>(macro_binding.get());
	} else {
		BindingAlias alias(catalog_name, schema_name, table_name);
		binding = bind_context.GetBinding(alias, column_name, error);
	}
	return binding;
}

void Binder::SetBindingMode(BindingMode mode) {
	global_binder_state->mode = mode;
}

BindingMode Binder::GetBindingMode() {
	return global_binder_state->mode;
}

void Binder::SetCanContainNulls(bool can_contain_nulls_p) {
	can_contain_nulls = can_contain_nulls_p;
}

void Binder::SetAlwaysRequireRebind() {
	auto &properties = GetStatementProperties();
	properties.always_require_rebind = true;
}

void Binder::AddTableName(string table_name) {
	global_binder_state->table_names.insert(std::move(table_name));
}

void Binder::AddReplacementScan(const string &table_name, unique_ptr<TableRef> replacement) {
	auto it = global_binder_state->replacement_scans.find(table_name);
	replacement->column_name_alias.clear();
	replacement->alias.clear();
	if (it == global_binder_state->replacement_scans.end()) {
		global_binder_state->replacement_scans[table_name] = std::move(replacement);
	} else {
		// A replacement scan by this name was previously registered, we can just use it
	}
}

const unordered_set<string> &Binder::GetTableNames() {
	return global_binder_state->table_names;
}

case_insensitive_map_t<unique_ptr<TableRef>> &Binder::GetReplacementScans() {
	return global_binder_state->replacement_scans;
}

// FIXME: this is extremely naive
void VerifyNotExcluded(const ParsedExpression &root_expr) {
	ParsedExpressionIterator::VisitExpression<ColumnRefExpression>(
	    root_expr, [&](const ColumnRefExpression &column_ref) {
		    if (!column_ref.IsQualified()) {
			    return;
		    }
		    auto &table_name = column_ref.GetTableName();
		    if (table_name == "excluded") {
			    throw NotImplementedException(
			        "'excluded' qualified columns are not supported in the RETURNING clause yet");
		    }
	    });
}

BoundStatement Binder::BindReturning(vector<unique_ptr<ParsedExpression>> returning_list, TableCatalogEntry &table,
                                     const string &alias, idx_t update_table_index,
                                     unique_ptr<LogicalOperator> child_operator, virtual_column_map_t virtual_columns) {
	vector<LogicalType> types;
	vector<string> names;

	auto binder = Binder::CreateBinder(context);

	vector<ColumnIndex> bound_columns;
	idx_t column_count = 0;
	for (auto &col : table.GetColumns().Logical()) {
		names.push_back(col.Name());
		types.push_back(col.Type());
		if (!col.Generated()) {
			bound_columns.emplace_back(column_count);
		}
		column_count++;
	}

	binder->bind_context.AddBaseTable(update_table_index, alias, names, types, bound_columns, table,
	                                  std::move(virtual_columns));
	ReturningBinder returning_binder(*binder, context);

	vector<unique_ptr<Expression>> projection_expressions;
	LogicalType result_type;
	vector<unique_ptr<ParsedExpression>> new_returning_list;
	BoundStatement result;
	binder->ExpandStarExpressions(returning_list, new_returning_list);
	for (auto &returning_expr : new_returning_list) {
		VerifyNotExcluded(*returning_expr);
		auto expr = returning_binder.Bind(returning_expr, &result_type);
		result.names.push_back(expr->GetName());
		result.types.push_back(result_type);
		projection_expressions.push_back(std::move(expr));
	}
	if (new_returning_list.empty()) {
		throw BinderException("RETURNING list is empty!");
	}
	auto projection = make_uniq<LogicalProjection>(GenerateTableIndex(), std::move(projection_expressions));
	projection->AddChild(std::move(child_operator));
	D_ASSERT(result.types.size() == result.names.size());
	result.plan = std::move(projection);
	// If an insert/delete/update statement returns data, there are sometimes issues with streaming results
	// where the data modification doesn't take place until the streamed result is exhausted. Once a row is
	// returned, it should be guaranteed that the row has been inserted.
	// see https://github.com/duckdb/duckdb/issues/8310
	auto &properties = GetStatementProperties();
	properties.output_type = QueryResultOutputType::FORCE_MATERIALIZED;
	properties.return_type = StatementReturnType::QUERY_RESULT;
	return result;
}

optional_ptr<CatalogEntry> Binder::GetCatalogEntry(const string &catalog, const string &schema,
                                                   const EntryLookupInfo &lookup_info,
                                                   OnEntryNotFound on_entry_not_found) {
	return entry_retriever.GetEntry(catalog, schema, lookup_info, on_entry_not_found);
}

} // namespace duckdb
