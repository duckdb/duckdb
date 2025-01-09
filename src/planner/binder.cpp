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

Binder &Binder::GetRootBinder() {
	reference<Binder> root = *this;
	while (root.get().parent) {
		root = *root.get().parent;
	}
	return root.get();
}

idx_t Binder::GetBinderDepth() const {
	const_reference<Binder> root = *this;
	idx_t depth = 1;
	while (root.get().parent) {
		depth++;
		root = *root.get().parent;
	}
	return depth;
}

shared_ptr<Binder> Binder::CreateBinder(ClientContext &context, optional_ptr<Binder> parent, BinderType binder_type) {
	auto depth = parent ? parent->GetBinderDepth() : 0;
	if (depth > context.config.max_expression_depth) {
		throw BinderException("Max expression depth limit of %lld exceeded. Use \"SET max_expression_depth TO x\" to "
		                      "increase the maximum expression depth.",
		                      context.config.max_expression_depth);
	}
	return shared_ptr<Binder>(new Binder(context, parent ? parent->shared_from_this() : nullptr, binder_type));
}

Binder::Binder(ClientContext &context, shared_ptr<Binder> parent_p, BinderType binder_type)
    : context(context), bind_context(*this), parent(std::move(parent_p)), bound_tables(0), binder_type(binder_type),
      entry_retriever(context) {
	if (parent) {
		entry_retriever.Inherit(parent->entry_retriever);

		// We have to inherit macro and lambda parameter bindings and from the parent binder, if there is a parent.
		macro_binding = parent->macro_binding;
		lambda_bindings = parent->lambda_bindings;

		if (binder_type == BinderType::REGULAR_BINDER) {
			// We have to inherit CTE bindings from the parent bind_context, if there is a parent.
			bind_context.SetCTEBindings(parent->bind_context.GetCTEBindings());
			bind_context.cte_references = parent->bind_context.cte_references;
			parameters = parent->parameters;
		}
	}
}

unique_ptr<BoundCTENode> Binder::BindMaterializedCTE(CommonTableExpressionMap &cte_map) {
	// Extract materialized CTEs from cte_map
	vector<unique_ptr<CTENode>> materialized_ctes;
	for (auto &cte : cte_map.map) {
		auto &cte_entry = cte.second;
		if (cte_entry->materialized == CTEMaterialize::CTE_MATERIALIZE_ALWAYS) {
			auto mat_cte = make_uniq<CTENode>();
			mat_cte->ctename = cte.first;
			mat_cte->query = cte_entry->query->node->Copy();
			mat_cte->aliases = cte_entry->aliases;
			materialized_ctes.push_back(std::move(mat_cte));
		}
	}

	if (materialized_ctes.empty()) {
		return nullptr;
	}

	unique_ptr<CTENode> cte_root = nullptr;
	while (!materialized_ctes.empty()) {
		unique_ptr<CTENode> node_result;
		node_result = std::move(materialized_ctes.back());
		node_result->cte_map = cte_map.Copy();
		if (cte_root) {
			node_result->child = std::move(cte_root);
		} else {
			node_result->child = nullptr;
		}
		cte_root = std::move(node_result);
		materialized_ctes.pop_back();
	}

	AddCTEMap(cte_map);
	auto bound_cte = BindCTE(cte_root->Cast<CTENode>());

	return bound_cte;
}

template <class T>
BoundStatement Binder::BindWithCTE(T &statement) {
	BoundStatement bound_statement;
	auto bound_cte = BindMaterializedCTE(statement.template Cast<T>().cte_map);
	if (bound_cte) {
		reference<BoundCTENode> tail_ref = *bound_cte;

		while (tail_ref.get().child && tail_ref.get().child->type == QueryNodeType::CTE_NODE) {
			tail_ref = tail_ref.get().child->Cast<BoundCTENode>();
		}

		auto &tail = tail_ref.get();
		bound_statement = tail.child_binder->Bind(statement.template Cast<T>());

		tail.types = bound_statement.types;
		tail.names = bound_statement.names;

		for (auto &c : tail.query_binder->correlated_columns) {
			tail.child_binder->AddCorrelatedColumn(c);
		}
		MoveCorrelatedExpressions(*tail.child_binder);

		auto plan = std::move(bound_statement.plan);
		bound_statement.plan = CreatePlan(*bound_cte, std::move(plan));
	} else {
		bound_statement = Bind(statement.template Cast<T>());
	}
	return bound_statement;
}

BoundStatement Binder::Bind(SQLStatement &statement) {
	root_statement = &statement;
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
	default: // LCOV_EXCL_START
		throw NotImplementedException("Unimplemented statement type \"%s\" for Bind",
		                              StatementTypeToString(statement.type));
	} // LCOV_EXCL_STOP
}

void Binder::AddCTEMap(CommonTableExpressionMap &cte_map) {
	for (auto &cte_it : cte_map.map) {
		AddCTE(cte_it.first, *cte_it.second);
	}
}

static void GetTableRefCountsNode(case_insensitive_map_t<idx_t> &cte_ref_counts, QueryNode &node);

static void GetTableRefCountsExpr(case_insensitive_map_t<idx_t> &cte_ref_counts, ParsedExpression &expr) {
	if (expr.GetExpressionType() == ExpressionType::SUBQUERY) {
		auto &subquery = expr.Cast<SubqueryExpression>();
		GetTableRefCountsNode(cte_ref_counts, *subquery.subquery->node);
	} else {
		ParsedExpressionIterator::EnumerateChildren(
		    expr, [&](ParsedExpression &expr) { GetTableRefCountsExpr(cte_ref_counts, expr); });
	}
}

static void GetTableRefCountsNode(case_insensitive_map_t<idx_t> &cte_ref_counts, QueryNode &node) {
	ParsedExpressionIterator::EnumerateQueryNodeChildren(
	    node, [&](unique_ptr<ParsedExpression> &child) { GetTableRefCountsExpr(cte_ref_counts, *child); },
	    [&](TableRef &ref) {
		    if (ref.type != TableReferenceType::BASE_TABLE) {
			    return;
		    }
		    auto cte_ref_counts_it = cte_ref_counts.find(ref.Cast<BaseTableRef>().table_name);
		    if (cte_ref_counts_it != cte_ref_counts.end()) {
			    cte_ref_counts_it->second++;
		    }
	    });
}

static bool ParsedExpressionIsAggregate(Binder &binder, const ParsedExpression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::FUNCTION) {
		auto &function = expr.Cast<FunctionExpression>();
		QueryErrorContext error_context;
		auto entry = binder.GetCatalogEntry(CatalogType::AGGREGATE_FUNCTION_ENTRY, function.catalog, function.schema,
		                                    function.function_name, OnEntryNotFound::RETURN_NULL, error_context);
		if (entry && entry->type == CatalogType::AGGREGATE_FUNCTION_ENTRY) {
			return true;
		}
	}
	bool is_aggregate = false;
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](const ParsedExpression &child) { is_aggregate |= ParsedExpressionIsAggregate(binder, child); });
	return is_aggregate;
}

bool Binder::OptimizeCTEs(QueryNode &node) {
	D_ASSERT(context.config.enable_optimizer);

	// only applies to nodes that have at least one CTE
	auto &cte_map = node.cte_map.map;
	if (cte_map.empty()) {
		return false;
	}

	// initialize counts with the CTE names
	case_insensitive_map_t<idx_t> cte_ref_counts;
	for (auto &cte : cte_map) {
		cte_ref_counts[cte.first];
	}

	// count the references of each CTE
	GetTableRefCountsNode(cte_ref_counts, node);

	// determine for each CTE whether it should be materialized
	bool result = false;
	for (auto &cte : cte_map) {
		if (cte.second->materialized != CTEMaterialize::CTE_MATERIALIZE_DEFAULT) {
			continue; // only triggers when nothing is specified
		}
		if (bind_context.GetCTEBinding(cte.first)) {
			continue; // there's a CTE in the bind context with an overlapping name, we can't also materialize this
		}

		auto cte_ref_counts_it = cte_ref_counts.find(cte.first);
		D_ASSERT(cte_ref_counts_it != cte_ref_counts.end());

		// only applies to CTEs that are referenced more than once
		if (cte_ref_counts_it->second <= 1) {
			continue;
		}

		// if the cte is a SELECT node
		if (cte.second->query->node->type != QueryNodeType::SELECT_NODE) {
			continue;
		}

		// we materialize if the CTE ends in an aggregation
		auto &cte_node = cte.second->query->node->Cast<SelectNode>();
		bool materialize = !cte_node.groups.group_expressions.empty() || !cte_node.groups.grouping_sets.empty();
		// or has a distinct modifier
		for (auto &modifier : cte_node.modifiers) {
			if (materialize) {
				break;
			}
			if (modifier->type == ResultModifierType::DISTINCT_MODIFIER) {
				materialize = true;
			}
		}
		for (auto &sel : cte_node.select_list) {
			if (materialize) {
				break;
			}
			materialize |= ParsedExpressionIsAggregate(*this, *sel);
		}

		if (materialize) {
			cte.second->materialized = CTEMaterialize::CTE_MATERIALIZE_ALWAYS;
			result = true;
		}
	}
	return result;
}

unique_ptr<BoundQueryNode> Binder::BindNode(QueryNode &node) {
	// first we visit the set of CTEs and add them to the bind context
	AddCTEMap(node.cte_map);
	// now we bind the node
	unique_ptr<BoundQueryNode> result;
	switch (node.type) {
	case QueryNodeType::SELECT_NODE:
		result = BindNode(node.Cast<SelectNode>());
		break;
	case QueryNodeType::RECURSIVE_CTE_NODE:
		result = BindNode(node.Cast<RecursiveCTENode>());
		break;
	case QueryNodeType::CTE_NODE:
		result = BindNode(node.Cast<CTENode>());
		break;
	default:
		D_ASSERT(node.type == QueryNodeType::SET_OPERATION_NODE);
		result = BindNode(node.Cast<SetOperationNode>());
		break;
	}
	return result;
}

BoundStatement Binder::Bind(QueryNode &node) {
	BoundStatement result;
	if (node.type != QueryNodeType::CTE_NODE && // Issue #13850 - Don't auto-materialize if users materialize (for now)
	    !Optimizer::OptimizerDisabled(context, OptimizerType::MATERIALIZED_CTE) && context.config.enable_optimizer &&
	    OptimizeCTEs(node)) {
		switch (node.type) {
		case QueryNodeType::SELECT_NODE:
			result = BindWithCTE(node.Cast<SelectNode>());
			break;
		case QueryNodeType::RECURSIVE_CTE_NODE:
			result = BindWithCTE(node.Cast<RecursiveCTENode>());
			break;
		case QueryNodeType::CTE_NODE:
			result = BindWithCTE(node.Cast<CTENode>());
			break;
		default:
			D_ASSERT(node.type == QueryNodeType::SET_OPERATION_NODE);
			result = BindWithCTE(node.Cast<SetOperationNode>());
			break;
		}
	} else {
		auto bound_node = BindNode(node);

		result.names = bound_node->names;
		result.types = bound_node->types;

		// and plan it
		result.plan = CreatePlan(*bound_node);
	}
	return result;
}

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundQueryNode &node) {
	switch (node.type) {
	case QueryNodeType::SELECT_NODE:
		return CreatePlan(node.Cast<BoundSelectNode>());
	case QueryNodeType::SET_OPERATION_NODE:
		return CreatePlan(node.Cast<BoundSetOperationNode>());
	case QueryNodeType::RECURSIVE_CTE_NODE:
		return CreatePlan(node.Cast<BoundRecursiveCTENode>());
	case QueryNodeType::CTE_NODE:
		return CreatePlan(node.Cast<BoundCTENode>());
	default:
		throw InternalException("Unsupported bound query node type");
	}
}

unique_ptr<BoundTableRef> Binder::Bind(TableRef &ref) {
	unique_ptr<BoundTableRef> result;
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
	case TableReferenceType::CTE:
	case TableReferenceType::INVALID:
	default:
		throw InternalException("Unknown table ref type (%s)", EnumUtil::ToString(ref.type));
	}
	result->sample = std::move(ref.sample);
	return result;
}

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundTableRef &ref) {
	unique_ptr<LogicalOperator> root;
	switch (ref.type) {
	case TableReferenceType::BASE_TABLE:
		root = CreatePlan(ref.Cast<BoundBaseTableRef>());
		break;
	case TableReferenceType::SUBQUERY:
		root = CreatePlan(ref.Cast<BoundSubqueryRef>());
		break;
	case TableReferenceType::JOIN:
		root = CreatePlan(ref.Cast<BoundJoinRef>());
		break;
	case TableReferenceType::TABLE_FUNCTION:
		root = CreatePlan(ref.Cast<BoundTableFunction>());
		break;
	case TableReferenceType::EMPTY_FROM:
		root = CreatePlan(ref.Cast<BoundEmptyTableRef>());
		break;
	case TableReferenceType::EXPRESSION_LIST:
		root = CreatePlan(ref.Cast<BoundExpressionListRef>());
		break;
	case TableReferenceType::COLUMN_DATA:
		root = CreatePlan(ref.Cast<BoundColumnDataRef>());
		break;
	case TableReferenceType::CTE:
		root = CreatePlan(ref.Cast<BoundCTERef>());
		break;
	case TableReferenceType::PIVOT:
		root = CreatePlan(ref.Cast<BoundPivotRef>());
		break;
	case TableReferenceType::DELIM_GET:
		root = CreatePlan(ref.Cast<BoundDelimGetRef>());
		break;
	case TableReferenceType::INVALID:
	default:
		throw InternalException("Unsupported bound table ref type (%s)", EnumUtil::ToString(ref.type));
	}
	// plan the sample clause
	if (ref.sample) {
		root = make_uniq<LogicalSample>(std::move(ref.sample), std::move(root));
	}
	return root;
}

void Binder::AddCTE(const string &name, CommonTableExpressionInfo &info) {
	D_ASSERT(!name.empty());
	auto entry = CTE_bindings.find(name);
	if (entry != CTE_bindings.end()) {
		throw InternalException("Duplicate CTE \"%s\" in query!", name);
	}
	CTE_bindings.insert(make_pair(name, reference<CommonTableExpressionInfo>(info)));
}

vector<reference<CommonTableExpressionInfo>> Binder::FindCTE(const string &name, bool skip) {
	auto entry = CTE_bindings.find(name);
	vector<reference<CommonTableExpressionInfo>> ctes;
	if (entry != CTE_bindings.end()) {
		if (!skip || entry->second.get().query->node->type == QueryNodeType::RECURSIVE_CTE_NODE) {
			ctes.push_back(entry->second);
		}
	}
	if (parent && binder_type == BinderType::REGULAR_BINDER) {
		auto parent_ctes = parent->FindCTE(name, name == alias);
		ctes.insert(ctes.end(), parent_ctes.begin(), parent_ctes.end());
	}
	return ctes;
}

bool Binder::CTEIsAlreadyBound(CommonTableExpressionInfo &cte) {
	if (bound_ctes.find(cte) != bound_ctes.end()) {
		return true;
	}
	if (parent && binder_type == BinderType::REGULAR_BINDER) {
		return parent->CTEIsAlreadyBound(cte);
	}
	return false;
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
	auto &root_binder = GetRootBinder();
	return root_binder.bound_tables++;
}

StatementProperties &Binder::GetStatementProperties() {
	auto &root_binder = GetRootBinder();
	return root_binder.prop;
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
	reference<Binder> root = *this;
	while (root.get().parent && root.get().binder_type == BinderType::REGULAR_BINDER) {
		root = *root.get().parent;
	}
	auto &root_binder = root.get();
	return root_binder.active_binders;
}

void Binder::AddUsingBindingSet(unique_ptr<UsingColumnSet> set) {
	auto &root_binder = GetRootBinder();
	root_binder.bind_context.AddUsingBindingSet(std::move(set));
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
	D_ASSERT(!lambda_bindings);
	if (macro_binding && table_name == macro_binding->GetAlias()) {
		binding = optional_ptr<Binding>(macro_binding.get());
	} else {
		BindingAlias alias(catalog_name, schema_name, table_name);
		binding = bind_context.GetBinding(alias, column_name, error);
	}
	return binding;
}

void Binder::SetBindingMode(BindingMode mode) {
	auto &root_binder = GetRootBinder();
	root_binder.mode = mode;
}

BindingMode Binder::GetBindingMode() {
	auto &root_binder = GetRootBinder();
	return root_binder.mode;
}

void Binder::SetCanContainNulls(bool can_contain_nulls_p) {
	can_contain_nulls = can_contain_nulls_p;
}

void Binder::SetAlwaysRequireRebind() {
	auto &properties = GetStatementProperties();
	properties.always_require_rebind = true;
}

void Binder::AddTableName(string table_name) {
	auto &root_binder = GetRootBinder();
	root_binder.table_names.insert(std::move(table_name));
}

void Binder::AddReplacementScan(const string &table_name, unique_ptr<TableRef> replacement) {
	auto &root_binder = GetRootBinder();
	auto it = root_binder.replacement_scans.find(table_name);
	replacement->column_name_alias.clear();
	replacement->alias.clear();
	if (it == root_binder.replacement_scans.end()) {
		root_binder.replacement_scans[table_name] = std::move(replacement);
	} else {
		// A replacement scan by this name was previously registered, we can just use it
	}
}

const unordered_set<string> &Binder::GetTableNames() {
	auto &root_binder = GetRootBinder();
	return root_binder.table_names;
}

case_insensitive_map_t<unique_ptr<TableRef>> &Binder::GetReplacementScans() {
	auto &root_binder = GetRootBinder();
	return root_binder.replacement_scans;
}

// FIXME: this is extremely naive
void VerifyNotExcluded(ParsedExpression &expr) {
	if (expr.GetExpressionType() == ExpressionType::COLUMN_REF) {
		auto &column_ref = expr.Cast<ColumnRefExpression>();
		if (!column_ref.IsQualified()) {
			return;
		}
		auto &table_name = column_ref.GetTableName();
		if (table_name == "excluded") {
			throw NotImplementedException("'excluded' qualified columns are not supported in the RETURNING clause yet");
		}
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](const ParsedExpression &child) { VerifyNotExcluded((ParsedExpression &)child); });
}

BoundStatement Binder::BindReturning(vector<unique_ptr<ParsedExpression>> returning_list, TableCatalogEntry &table,
                                     const string &alias, idx_t update_table_index,
                                     unique_ptr<LogicalOperator> child_operator, BoundStatement result) {

	vector<LogicalType> types;
	vector<std::string> names;

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

	binder->bind_context.AddBaseTable(update_table_index, alias, names, types, bound_columns, table, false);
	ReturningBinder returning_binder(*binder, context);

	vector<unique_ptr<Expression>> projection_expressions;
	LogicalType result_type;
	vector<unique_ptr<ParsedExpression>> new_returning_list;
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
	properties.allow_stream_result = false;
	properties.return_type = StatementReturnType::QUERY_RESULT;
	return result;
}

optional_ptr<CatalogEntry> Binder::GetCatalogEntry(CatalogType type, const string &catalog, const string &schema,
                                                   const string &name, OnEntryNotFound on_entry_not_found,
                                                   QueryErrorContext &error_context) {
	return entry_retriever.GetEntry(type, catalog, schema, name, on_entry_not_found, error_context);
}

} // namespace duckdb
