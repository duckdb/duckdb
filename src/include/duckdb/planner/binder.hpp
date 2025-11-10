//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/result_modifier.hpp"
#include "duckdb/parser/tableref/delimgetref.hpp"
#include "duckdb/parser/tokens.hpp"
#include "duckdb/planner/bind_context.hpp"
#include "duckdb/planner/bound_statement.hpp"
#include "duckdb/planner/bound_tokens.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/planner/bound_constraint.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/common/enums/copy_option_mode.hpp"

//! fwd declare
namespace duckdb_re2 {
class RE2;
} // namespace duckdb_re2

namespace duckdb {
class BoundResultModifier;
class BoundSelectNode;
class ClientContext;
class ExpressionBinder;
class LimitModifier;
class OrderBinder;
class TableCatalogEntry;
class ViewCatalogEntry;
class TableMacroCatalogEntry;
class UpdateSetInfo;
class LogicalProjection;
class LogicalVacuum;

class ColumnList;
class ExternalDependency;
class TableFunction;
class TableStorageInfo;
class BoundConstraint;
class AtClause;
class BoundAtClause;

struct CreateInfo;
struct BoundCreateTableInfo;
struct BoundOnConflictInfo;
struct CommonTableExpressionInfo;
struct BoundParameterMap;
struct BoundPragmaInfo;
struct BoundLimitNode;
struct EntryLookupInfo;
struct PivotColumnEntry;
struct UnpivotEntry;
struct CopyInfo;
struct CopyOption;
struct BoundSetOpChild;
struct BoundCTEData;

template <class T, class INDEX_TYPE>
class IndexVector;

enum class BindingMode : uint8_t {
	STANDARD_BINDING,
	EXTRACT_NAMES,
	EXTRACT_REPLACEMENT_SCANS,
	EXTRACT_QUALIFIED_NAMES
};
enum class BinderType : uint8_t { REGULAR_BINDER, VIEW_BINDER };

struct CorrelatedColumnInfo {
	ColumnBinding binding;
	LogicalType type;
	string name;
	idx_t depth;

	// NOLINTNEXTLINE - work-around bug in clang-tidy
	CorrelatedColumnInfo(ColumnBinding binding, LogicalType type_p, string name_p, idx_t depth)
	    : binding(binding), type(std::move(type_p)), name(std::move(name_p)), depth(depth) {
	}
	explicit CorrelatedColumnInfo(BoundColumnRefExpression &expr)
	    : CorrelatedColumnInfo(expr.binding, expr.return_type, expr.GetName(), expr.depth) {
	}

	bool operator==(const CorrelatedColumnInfo &rhs) const {
		return binding == rhs.binding;
	}
};

struct CorrelatedColumns {
private:
	using container_type = vector<CorrelatedColumnInfo>;

public:
	CorrelatedColumns() : delim_index(1ULL << 63) {
	}

	void AddColumn(container_type::value_type info) {
		// Add to beginning
		correlated_columns.insert(correlated_columns.begin(), std::move(info));
		delim_index++;
	}

	void SetDelimIndexToZero() {
		delim_index = 0;
	}

	idx_t GetDelimIndex() const {
		return delim_index;
	}

	const container_type::value_type &operator[](const idx_t &index) const {
		return correlated_columns.at(index);
	}

	idx_t size() const { // NOLINT: match stl case
		return correlated_columns.size();
	}

	bool empty() const { // NOLINT: match stl case
		return correlated_columns.empty();
	}

	void clear() { // NOLINT: match stl case
		correlated_columns.clear();
	}

	container_type::iterator begin() { // NOLINT: match stl case
		return correlated_columns.begin();
	}

	container_type::iterator end() { // NOLINT: match stl case
		return correlated_columns.end();
	}

	container_type::const_iterator begin() const { // NOLINT: match stl case
		return correlated_columns.begin();
	}

	container_type::const_iterator end() const { // NOLINT: match stl case
		return correlated_columns.end();
	}

private:
	container_type correlated_columns;
	idx_t delim_index;
};

//! GlobalBinderState is state shared over the ENTIRE query, including subqueries, views, etc
struct GlobalBinderState {
	//! The count of bound_tables
	idx_t bound_tables = 0;
	//! Statement properties
	StatementProperties prop;
	//! Binding mode
	BindingMode mode = BindingMode::STANDARD_BINDING;
	//! Table names extracted for BindingMode::EXTRACT_NAMES or BindingMode::EXTRACT_QUALIFIED_NAMES.
	unordered_set<string> table_names;
	//! Replacement Scans extracted for BindingMode::EXTRACT_REPLACEMENT_SCANS
	case_insensitive_map_t<unique_ptr<TableRef>> replacement_scans;
	//! Using column sets
	vector<unique_ptr<UsingColumnSet>> using_column_sets;
	//! The set of parameter expressions bound by this binder
	optional_ptr<BoundParameterMap> parameters;
};

// QueryBinderState is state shared WITHIN a query, a new query-binder state is created when binding inside e.g. a view
struct QueryBinderState {
	//! The vector of active binders
	vector<reference<ExpressionBinder>> active_binders;
};

//! Bind the parsed query tree to the actual columns present in the catalog.
/*!
  The binder is responsible for binding tables and columns to actual physical
  tables and columns in the catalog. In the process, it also resolves types of
  all expressions.
*/
class Binder : public enable_shared_from_this<Binder> {
	friend class ExpressionBinder;
	friend class RecursiveDependentJoinPlanner;

public:
	DUCKDB_API static shared_ptr<Binder> CreateBinder(ClientContext &context, optional_ptr<Binder> parent = nullptr,
	                                                  BinderType binder_type = BinderType::REGULAR_BINDER);

	//! The client context
	ClientContext &context;
	//! The bind context
	BindContext bind_context;
	//! The set of correlated columns bound by this binder (FIXME: this should probably be an unordered_set and not a
	//! vector)
	CorrelatedColumns correlated_columns;
	//! The alias for the currently processing subquery, if it exists
	string alias;
	//! Macro parameter bindings (if any)
	optional_ptr<DummyBinding> macro_binding;
	//! The intermediate lambda bindings to bind nested lambdas (if any)
	optional_ptr<vector<DummyBinding>> lambda_bindings;

	unordered_map<idx_t, LogicalOperator *> recursive_ctes;

public:
	DUCKDB_API BoundStatement Bind(SQLStatement &statement);
	DUCKDB_API BoundStatement Bind(QueryNode &node);

	unique_ptr<BoundCreateTableInfo> BindCreateTableInfo(unique_ptr<CreateInfo> info);
	unique_ptr<BoundCreateTableInfo> BindCreateTableInfo(unique_ptr<CreateInfo> info, SchemaCatalogEntry &schema);
	unique_ptr<BoundCreateTableInfo> BindCreateTableInfo(unique_ptr<CreateInfo> info, SchemaCatalogEntry &schema,
	                                                     vector<unique_ptr<Expression>> &bound_defaults);
	static unique_ptr<BoundCreateTableInfo> BindCreateTableCheckpoint(unique_ptr<CreateInfo> info,
	                                                                  SchemaCatalogEntry &schema);

	static vector<unique_ptr<BoundConstraint>> BindConstraints(ClientContext &context,
	                                                           const vector<unique_ptr<Constraint>> &constraints,
	                                                           const string &table_name, const ColumnList &columns);
	vector<unique_ptr<BoundConstraint>> BindConstraints(const vector<unique_ptr<Constraint>> &constraints,
	                                                    const string &table_name, const ColumnList &columns);
	vector<unique_ptr<BoundConstraint>> BindConstraints(const TableCatalogEntry &table);
	vector<unique_ptr<BoundConstraint>> BindNewConstraints(vector<unique_ptr<Constraint>> &constraints,
	                                                       const string &table_name, const ColumnList &columns);
	unique_ptr<BoundConstraint> BindConstraint(const Constraint &constraint, const string &table,
	                                           const ColumnList &columns);
	unique_ptr<BoundConstraint> BindUniqueConstraint(const Constraint &constraint, const string &table,
	                                                 const ColumnList &columns);

	BoundStatement BindAlterAddIndex(BoundStatement &result, CatalogEntry &entry, unique_ptr<AlterInfo> alter_info);

	void SetCatalogLookupCallback(catalog_entry_callback_t callback);
	void BindCreateViewInfo(CreateViewInfo &base);
	void SearchSchema(CreateInfo &info);
	SchemaCatalogEntry &BindSchema(CreateInfo &info);
	SchemaCatalogEntry &BindCreateFunctionInfo(CreateInfo &info);

	//! Check usage, and cast named parameters to their types
	static void BindNamedParameters(named_parameter_type_map_t &types, named_parameter_map_t &values,
	                                QueryErrorContext &error_context, string &func_name);
	unique_ptr<BoundPragmaInfo> BindPragma(PragmaInfo &info, QueryErrorContext error_context);

	BoundStatement Bind(TableRef &ref);

	//! Generates an unused index for a table
	idx_t GenerateTableIndex();

	optional_ptr<CatalogEntry> GetCatalogEntry(const string &catalog, const string &schema,
	                                           const EntryLookupInfo &lookup_info, OnEntryNotFound on_entry_not_found);

	//! Find all candidate common table expression by name; returns empty vector if none exists
	optional_ptr<CTEBinding> GetCTEBinding(const BindingAlias &name);

	//! Add the view to the set of currently bound views - used for detecting recursive view definitions
	void AddBoundView(ViewCatalogEntry &view);

	void PushExpressionBinder(ExpressionBinder &binder);
	void PopExpressionBinder();
	void SetActiveBinder(ExpressionBinder &binder);
	ExpressionBinder &GetActiveBinder();
	bool HasActiveBinder();

	vector<reference<ExpressionBinder>> &GetActiveBinders();

	void MergeCorrelatedColumns(CorrelatedColumns &other);
	//! Add a correlated column to this binder (if it does not exist)
	void AddCorrelatedColumn(const CorrelatedColumnInfo &info);

	unique_ptr<LogicalOperator> BindUpdateSet(LogicalOperator &op, unique_ptr<LogicalOperator> root,
	                                          UpdateSetInfo &set_info, TableCatalogEntry &table,
	                                          vector<PhysicalIndex> &columns);
	void BindUpdateSet(idx_t proj_index, unique_ptr<LogicalOperator> &root, UpdateSetInfo &set_info,
	                   TableCatalogEntry &table, vector<PhysicalIndex> &columns,
	                   vector<unique_ptr<Expression>> &update_expressions,
	                   vector<unique_ptr<Expression>> &projection_expressions);

	void BindVacuumTable(LogicalVacuum &vacuum, unique_ptr<LogicalOperator> &root);

	static void BindSchemaOrCatalog(ClientContext &context, string &catalog, string &schema);
	void BindLogicalType(LogicalType &type, optional_ptr<Catalog> catalog = nullptr,
	                     const string &schema = INVALID_SCHEMA);

	optional_ptr<Binding> GetMatchingBinding(const string &table_name, const string &column_name, ErrorData &error);
	optional_ptr<Binding> GetMatchingBinding(const string &schema_name, const string &table_name,
	                                         const string &column_name, ErrorData &error);
	optional_ptr<Binding> GetMatchingBinding(const string &catalog_name, const string &schema_name,
	                                         const string &table_name, const string &column_name, ErrorData &error);

	void SetBindingMode(BindingMode mode);
	BindingMode GetBindingMode();
	void AddTableName(string table_name);
	void AddReplacementScan(const string &table_name, unique_ptr<TableRef> replacement);
	const unordered_set<string> &GetTableNames();
	case_insensitive_map_t<unique_ptr<TableRef>> &GetReplacementScans();
	CatalogEntryRetriever &EntryRetriever() {
		return entry_retriever;
	}
	optional_ptr<BoundParameterMap> GetParameters();
	void SetParameters(BoundParameterMap &parameters);
	//! Returns a ColumnRefExpression after it was resolved (i.e. past the STAR expression/USING clauses)
	static optional_ptr<ParsedExpression> GetResolvedColumnExpression(ParsedExpression &root_expr);

	void SetCanContainNulls(bool can_contain_nulls);
	void SetAlwaysRequireRebind();

	StatementProperties &GetStatementProperties();
	static void ReplaceStarExpression(unique_ptr<ParsedExpression> &expr, unique_ptr<ParsedExpression> &replacement);
	static string ReplaceColumnsAlias(const string &alias, const string &column_name,
	                                  optional_ptr<duckdb_re2::RE2> regex);

	unique_ptr<LogicalOperator> UnionOperators(vector<unique_ptr<LogicalOperator>> nodes);

private:
	//! The parent binder (if any)
	shared_ptr<Binder> parent;
	//! What kind of node we are binding using this binder
	BinderType binder_type = BinderType::REGULAR_BINDER;
	//! Global binder state
	shared_ptr<GlobalBinderState> global_binder_state;
	//! Query binder state
	shared_ptr<QueryBinderState> query_binder_state;
	//! Whether or not the binder has any unplanned dependent joins that still need to be planned/flattened
	bool has_unplanned_dependent_joins = false;
	//! Whether or not outside dependent joins have been planned and flattened
	bool is_outside_flattened = true;
	//! Whether or not the binder can contain NULLs as the root of expressions
	bool can_contain_nulls = false;
	//! The set of bound views
	reference_set_t<ViewCatalogEntry> bound_views;
	//! Used to retrieve CatalogEntry's
	CatalogEntryRetriever entry_retriever;
	//! Unnamed subquery index
	idx_t unnamed_subquery_index = 1;
	//! Binder depth
	idx_t depth;

private:
	//! Determine the depth of the binder
	idx_t GetBinderDepth() const;
	//! Increase the depth of the binder
	void IncreaseDepth();
	//! Bind the expressions of generated columns to check for errors
	void BindGeneratedColumns(BoundCreateTableInfo &info);
	//! Bind the default values of the columns of a table
	void BindDefaultValues(const ColumnList &columns, vector<unique_ptr<Expression>> &bound_defaults,
	                       const string &catalog = "", const string &schema = "");
	//! Bind a limit value (LIMIT or OFFSET)
	BoundLimitNode BindLimitValue(OrderBinder &order_binder, unique_ptr<ParsedExpression> limit_val, bool is_percentage,
	                              bool is_offset);

	//! Move correlated expressions from the child binder to this binder
	void MoveCorrelatedExpressions(Binder &other);

	//! Tries to bind the table name with replacement scans
	BoundStatement BindWithReplacementScan(ClientContext &context, BaseTableRef &ref);

	template <class T>
	BoundStatement BindWithCTE(T &statement);
	BoundStatement Bind(SelectStatement &stmt);
	BoundStatement Bind(InsertStatement &stmt);
	BoundStatement Bind(CopyStatement &stmt, CopyToType copy_to_type);
	BoundStatement Bind(DeleteStatement &stmt);
	BoundStatement Bind(UpdateStatement &stmt);
	BoundStatement Bind(CreateStatement &stmt);
	BoundStatement Bind(DropStatement &stmt);
	BoundStatement Bind(AlterStatement &stmt);
	BoundStatement Bind(PrepareStatement &stmt);
	BoundStatement Bind(ExecuteStatement &stmt);
	BoundStatement Bind(TransactionStatement &stmt);
	BoundStatement Bind(PragmaStatement &stmt);
	BoundStatement Bind(ExplainStatement &stmt);
	BoundStatement Bind(VacuumStatement &stmt);
	BoundStatement Bind(RelationStatement &stmt);
	BoundStatement Bind(CallStatement &stmt);
	BoundStatement Bind(ExportStatement &stmt);
	BoundStatement Bind(ExtensionStatement &stmt);
	BoundStatement Bind(SetStatement &stmt);
	BoundStatement Bind(SetVariableStatement &stmt);
	BoundStatement Bind(ResetVariableStatement &stmt);
	BoundStatement Bind(LoadStatement &stmt);
	BoundStatement Bind(LogicalPlanStatement &stmt);
	BoundStatement Bind(AttachStatement &stmt);
	BoundStatement Bind(DetachStatement &stmt);
	BoundStatement Bind(CopyDatabaseStatement &stmt);
	BoundStatement Bind(UpdateExtensionsStatement &stmt);
	BoundStatement Bind(MergeIntoStatement &stmt);

	void BindRowIdColumns(TableCatalogEntry &table, LogicalGet &get, vector<unique_ptr<Expression>> &expressions);
	BoundStatement BindReturning(vector<unique_ptr<ParsedExpression>> returning_list, TableCatalogEntry &table,
	                             const string &alias, idx_t update_table_index,
	                             unique_ptr<LogicalOperator> child_operator,
	                             virtual_column_map_t virtual_columns = virtual_column_map_t());

	unique_ptr<QueryNode> BindTableMacro(FunctionExpression &function, TableMacroCatalogEntry &macro_func, idx_t depth);

	BoundStatement BindCTE(const string &ctename, CommonTableExpressionInfo &info);

	BoundStatement BindNode(SelectNode &node);
	BoundStatement BindNode(SetOperationNode &node);
	BoundStatement BindNode(RecursiveCTENode &node);
	BoundStatement BindNode(QueryNode &node);
	BoundStatement BindNode(StatementNode &node);

	unique_ptr<LogicalOperator> VisitQueryNode(BoundQueryNode &node, unique_ptr<LogicalOperator> root);
	unique_ptr<LogicalOperator> CreatePlan(BoundSelectNode &statement);
	unique_ptr<LogicalOperator> CreatePlan(BoundSetOperationNode &node);
	unique_ptr<LogicalOperator> CreatePlan(BoundQueryNode &node);

	void BuildUnionByNameInfo(BoundSetOperationNode &result);

	BoundStatement BindJoin(Binder &parent, TableRef &ref);
	BoundStatement Bind(BaseTableRef &ref);
	BoundStatement Bind(BoundRefWrapper &ref);
	BoundStatement Bind(JoinRef &ref);
	BoundStatement Bind(SubqueryRef &ref);
	BoundStatement Bind(TableFunctionRef &ref);
	BoundStatement Bind(EmptyTableRef &ref);
	BoundStatement Bind(DelimGetRef &ref);
	BoundStatement Bind(ExpressionListRef &ref);
	BoundStatement Bind(ColumnDataRef &ref);
	BoundStatement Bind(PivotRef &expr);
	BoundStatement Bind(ShowRef &ref);

	unique_ptr<SelectNode> BindPivot(PivotRef &expr, vector<unique_ptr<ParsedExpression>> all_columns);
	unique_ptr<SelectNode> BindUnpivot(Binder &child_binder, PivotRef &expr,
	                                   vector<unique_ptr<ParsedExpression>> all_columns,
	                                   unique_ptr<ParsedExpression> &where_clause);
	BoundStatement BindBoundPivot(PivotRef &expr);
	void ExtractUnpivotEntries(Binder &child_binder, PivotColumnEntry &entry, vector<UnpivotEntry> &unpivot_entries);
	void ExtractUnpivotColumnName(ParsedExpression &expr, vector<string> &result);

	unique_ptr<BoundAtClause> BindAtClause(optional_ptr<AtClause> at_clause);

	bool BindTableFunctionParameters(TableFunctionCatalogEntry &table_function,
	                                 vector<unique_ptr<ParsedExpression>> &expressions, vector<LogicalType> &arguments,
	                                 vector<Value> &parameters, named_parameter_map_t &named_parameters,
	                                 BoundStatement &subquery, ErrorData &error);
	void BindTableInTableOutFunction(vector<unique_ptr<ParsedExpression>> &expressions, BoundStatement &subquery);
	BoundStatement BindTableFunction(TableFunction &function, vector<Value> parameters);
	BoundStatement BindTableFunctionInternal(TableFunction &table_function, const TableFunctionRef &ref,
	                                         vector<Value> parameters, named_parameter_map_t named_parameters,
	                                         vector<LogicalType> input_table_types, vector<string> input_table_names);

	unique_ptr<LogicalOperator> CreatePlan(BoundJoinRef &ref);

	BoundStatement BindCopyTo(CopyStatement &stmt, const CopyFunction &function, CopyToType copy_to_type);
	BoundStatement BindCopyFrom(CopyStatement &stmt, const CopyFunction &function);
	void BindCopyOptions(CopyInfo &info);
	case_insensitive_map_t<CopyOption> GetFullCopyOptionsList(const CopyFunction &function, CopyOptionMode mode);

	void PrepareModifiers(OrderBinder &order_binder, QueryNode &statement, BoundQueryNode &result);
	void BindModifiers(BoundQueryNode &result, idx_t table_index, const vector<string> &names,
	                   const vector<LogicalType> &sql_types, const SelectBindState &bind_state);

	unique_ptr<BoundResultModifier> BindLimit(OrderBinder &order_binder, LimitModifier &limit_mod);
	unique_ptr<BoundResultModifier> BindLimitPercent(OrderBinder &order_binder, LimitPercentModifier &limit_mod);
	unique_ptr<Expression> BindOrderExpression(OrderBinder &order_binder, unique_ptr<ParsedExpression> expr);

	unique_ptr<LogicalOperator> PlanFilter(unique_ptr<Expression> condition, unique_ptr<LogicalOperator> root);

	void PlanSubqueries(unique_ptr<Expression> &expr, unique_ptr<LogicalOperator> &root);
	unique_ptr<Expression> PlanSubquery(BoundSubqueryExpression &expr, unique_ptr<LogicalOperator> &root);
	unique_ptr<LogicalOperator> PlanLateralJoin(unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right,
	                                            CorrelatedColumns &correlated_columns,
	                                            JoinType join_type = JoinType::INNER,
	                                            unique_ptr<Expression> condition = nullptr);

	unique_ptr<LogicalOperator> CastLogicalOperatorToTypes(const vector<LogicalType> &source_types,
	                                                       const vector<LogicalType> &target_types,
	                                                       unique_ptr<LogicalOperator> op);

	BindingAlias FindBinding(const string &using_column, const string &join_side);
	bool TryFindBinding(const string &using_column, const string &join_side, BindingAlias &result);

	void AddUsingBindingSet(unique_ptr<UsingColumnSet> set);
	BindingAlias RetrieveUsingBinding(Binder &current_binder, optional_ptr<UsingColumnSet> current_set,
	                                  const string &column_name, const string &join_side);

	void ExpandStarExpressions(vector<unique_ptr<ParsedExpression>> &select_list,
	                           vector<unique_ptr<ParsedExpression>> &new_select_list);
	void ExpandStarExpression(unique_ptr<ParsedExpression> expr, vector<unique_ptr<ParsedExpression>> &new_select_list);
	StarExpressionType FindStarExpression(unique_ptr<ParsedExpression> &expr, StarExpression **star, bool is_root,
	                                      bool in_columns);
	void ReplaceUnpackedStarExpression(unique_ptr<ParsedExpression> &expr,
	                                   vector<unique_ptr<ParsedExpression>> &replacements, StarExpression &star,
	                                   optional_ptr<duckdb_re2::RE2> regex);
	void BindWhereStarExpression(unique_ptr<ParsedExpression> &expr);

	//! If only a schema name is provided (e.g. "a.b") then figure out if "a" is a schema or a catalog name
	void BindSchemaOrCatalog(string &catalog_name, string &schema_name);
	static void BindSchemaOrCatalog(CatalogEntryRetriever &retriever, string &catalog, string &schema);
	const string BindCatalog(string &catalog_name);
	SchemaCatalogEntry &BindCreateSchema(CreateInfo &info);

	vector<CatalogSearchEntry> GetSearchPath(Catalog &catalog, const string &schema_name);

	LogicalType BindLogicalTypeInternal(const LogicalType &type, optional_ptr<Catalog> catalog, const string &schema);

	BoundStatement BindSelectNode(SelectNode &statement, BoundStatement from_table);

	unique_ptr<LogicalOperator> BindCopyDatabaseSchema(Catalog &source_catalog, const string &target_database_name);
	unique_ptr<LogicalOperator> BindCopyDatabaseData(Catalog &source_catalog, const string &target_database_name);

	BoundStatement BindShowQuery(ShowRef &ref);
	BoundStatement BindShowTable(ShowRef &ref);
	BoundStatement BindSummarize(ShowRef &ref);

	void BindInsertColumnList(TableCatalogEntry &table, vector<string> &columns, bool default_values,
	                          vector<LogicalIndex> &named_column_map, vector<LogicalType> &expected_types,
	                          IndexVector<idx_t, PhysicalIndex> &column_index_map);
	void TryReplaceDefaultExpression(unique_ptr<ParsedExpression> &expr, const ColumnDefinition &column);
	void ExpandDefaultInValuesList(InsertStatement &stmt, TableCatalogEntry &table,
	                               optional_ptr<ExpressionListRef> values_list,
	                               const vector<LogicalIndex> &named_column_map);
	unique_ptr<BoundMergeIntoAction> BindMergeAction(LogicalMergeInto &merge_into, TableCatalogEntry &table,
	                                                 LogicalGet &get, idx_t proj_index,
	                                                 vector<unique_ptr<Expression>> &expressions,
	                                                 unique_ptr<LogicalOperator> &root, MergeIntoAction &action,
	                                                 const vector<BindingAlias> &source_aliases,
	                                                 const vector<string> &source_names);

	unique_ptr<MergeIntoStatement> GenerateMergeInto(InsertStatement &stmt, TableCatalogEntry &table);

	static void CheckInsertColumnCountMismatch(idx_t expected_columns, idx_t result_columns, bool columns_provided,
	                                           const string &tname);

	BoundCTEData PrepareCTE(const string &ctename, CommonTableExpressionInfo &statement);
	BoundStatement FinishCTE(BoundCTEData &bound_cte, BoundStatement child_data);

private:
	Binder(ClientContext &context, shared_ptr<Binder> parent, BinderType binder_type);
};

} // namespace duckdb
