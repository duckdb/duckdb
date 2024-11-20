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
#include "duckdb/planner/joinside.hpp"
#include "duckdb/planner/bound_constraint.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/tableref/bound_delimgetref.hpp"

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

struct CreateInfo;
struct BoundCreateTableInfo;
struct CommonTableExpressionInfo;
struct BoundParameterMap;
struct BoundPragmaInfo;
struct BoundLimitNode;
struct PivotColumnEntry;
struct UnpivotEntry;

enum class BindingMode : uint8_t { STANDARD_BINDING, EXTRACT_NAMES, EXTRACT_REPLACEMENT_SCANS };
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
	//! A mapping of names to common table expressions
	case_insensitive_map_t<reference<CommonTableExpressionInfo>> CTE_bindings; // NOLINT
	//! The CTEs that have already been bound
	reference_set_t<CommonTableExpressionInfo> bound_ctes;
	//! The bind context
	BindContext bind_context;
	//! The set of correlated columns bound by this binder (FIXME: this should probably be an unordered_set and not a
	//! vector)
	vector<CorrelatedColumnInfo> correlated_columns;
	//! The set of parameter expressions bound by this binder
	optional_ptr<BoundParameterMap> parameters;
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
	unique_ptr<BoundConstraint> BindConstraint(Constraint &constraint, const string &table, const ColumnList &columns);
	unique_ptr<BoundConstraint> BindUniqueConstraint(Constraint &constraint, const string &table,
	                                                 const ColumnList &columns);

	BoundStatement BindAlterAddIndex(BoundStatement &result, CatalogEntry &entry, unique_ptr<AlterInfo> alter_info);

	void SetCatalogLookupCallback(catalog_entry_callback_t callback);
	void BindCreateViewInfo(CreateViewInfo &base);
	SchemaCatalogEntry &BindSchema(CreateInfo &info);
	SchemaCatalogEntry &BindCreateFunctionInfo(CreateInfo &info);

	//! Check usage, and cast named parameters to their types
	static void BindNamedParameters(named_parameter_type_map_t &types, named_parameter_map_t &values,
	                                QueryErrorContext &error_context, string &func_name);
	unique_ptr<BoundPragmaInfo> BindPragma(PragmaInfo &info, QueryErrorContext error_context);

	unique_ptr<BoundTableRef> Bind(TableRef &ref);
	unique_ptr<LogicalOperator> CreatePlan(BoundTableRef &ref);

	//! Generates an unused index for a table
	idx_t GenerateTableIndex();

	optional_ptr<CatalogEntry> GetCatalogEntry(CatalogType type, const string &catalog, const string &schema,
	                                           const string &name, OnEntryNotFound on_entry_not_found,
	                                           QueryErrorContext &error_context);

	//! Add a common table expression to the binder
	void AddCTE(const string &name, CommonTableExpressionInfo &cte);
	//! Find all candidate common table expression by name; returns empty vector if none exists
	vector<reference<CommonTableExpressionInfo>> FindCTE(const string &name, bool skip = false);

	bool CTEIsAlreadyBound(CommonTableExpressionInfo &cte);

	//! Add the view to the set of currently bound views - used for detecting recursive view definitions
	void AddBoundView(ViewCatalogEntry &view);

	void PushExpressionBinder(ExpressionBinder &binder);
	void PopExpressionBinder();
	void SetActiveBinder(ExpressionBinder &binder);
	ExpressionBinder &GetActiveBinder();
	bool HasActiveBinder();

	vector<reference<ExpressionBinder>> &GetActiveBinders();

	void MergeCorrelatedColumns(vector<CorrelatedColumnInfo> &other);
	//! Add a correlated column to this binder (if it does not exist)
	void AddCorrelatedColumn(const CorrelatedColumnInfo &info);

	unique_ptr<LogicalOperator> BindUpdateSet(LogicalOperator &op, unique_ptr<LogicalOperator> root,
	                                          UpdateSetInfo &set_info, TableCatalogEntry &table,
	                                          vector<PhysicalIndex> &columns);
	void BindDoUpdateSetExpressions(const string &table_alias, LogicalInsert &insert, UpdateSetInfo &set_info,
	                                TableCatalogEntry &table, TableStorageInfo &storage_info);
	void BindOnConflictClause(LogicalInsert &insert, TableCatalogEntry &table, InsertStatement &stmt);

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
	optional_ptr<SQLStatement> GetRootStatement() {
		return root_statement;
	}
	CatalogEntryRetriever &EntryRetriever() {
		return entry_retriever;
	}

	void SetCanContainNulls(bool can_contain_nulls);
	void SetAlwaysRequireRebind();

	StatementProperties &GetStatementProperties();

private:
	//! The parent binder (if any)
	shared_ptr<Binder> parent;
	//! The vector of active binders
	vector<reference<ExpressionBinder>> active_binders;
	//! The count of bound_tables
	idx_t bound_tables;
	//! Whether or not the binder has any unplanned dependent joins that still need to be planned/flattened
	bool has_unplanned_dependent_joins = false;
	//! Whether or not outside dependent joins have been planned and flattened
	bool is_outside_flattened = true;
	//! What kind of node we are binding using this binder
	BinderType binder_type = BinderType::REGULAR_BINDER;
	//! Whether or not the binder can contain NULLs as the root of expressions
	bool can_contain_nulls = false;
	//! The root statement of the query that is currently being parsed
	optional_ptr<SQLStatement> root_statement;
	//! Binding mode
	BindingMode mode = BindingMode::STANDARD_BINDING;
	//! Table names extracted for BindingMode::EXTRACT_NAMES
	unordered_set<string> table_names;
	//! Replacement Scans extracted for BindingMode::EXTRACT_REPLACEMENT_SCANS
	case_insensitive_map_t<unique_ptr<TableRef>> replacement_scans;
	//! The set of bound views
	reference_set_t<ViewCatalogEntry> bound_views;
	//! Used to retrieve CatalogEntry's
	CatalogEntryRetriever entry_retriever;
	//! Unnamed subquery index
	idx_t unnamed_subquery_index = 1;
	//! Statement properties
	StatementProperties prop;

private:
	//! Get the root binder (binder with no parent)
	Binder &GetRootBinder();
	//! Determine the depth of the binder
	idx_t GetBinderDepth() const;
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
	unique_ptr<BoundTableRef> BindWithReplacementScan(ClientContext &context, BaseTableRef &ref);

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

	BoundStatement BindReturning(vector<unique_ptr<ParsedExpression>> returning_list, TableCatalogEntry &table,
	                             const string &alias, idx_t update_table_index,
	                             unique_ptr<LogicalOperator> child_operator, BoundStatement result);

	unique_ptr<QueryNode> BindTableMacro(FunctionExpression &function, TableMacroCatalogEntry &macro_func, idx_t depth);

	unique_ptr<BoundCTENode> BindMaterializedCTE(CommonTableExpressionMap &cte_map);
	unique_ptr<BoundCTENode> BindCTE(CTENode &statement);
	//! Materializes CTEs if this is expected to improve performance
	bool OptimizeCTEs(QueryNode &node);

	unique_ptr<BoundQueryNode> BindNode(SelectNode &node);
	unique_ptr<BoundQueryNode> BindNode(SetOperationNode &node);
	unique_ptr<BoundQueryNode> BindNode(RecursiveCTENode &node);
	unique_ptr<BoundQueryNode> BindNode(CTENode &node);
	unique_ptr<BoundQueryNode> BindNode(QueryNode &node);

	unique_ptr<LogicalOperator> VisitQueryNode(BoundQueryNode &node, unique_ptr<LogicalOperator> root);
	unique_ptr<LogicalOperator> CreatePlan(BoundRecursiveCTENode &node);
	unique_ptr<LogicalOperator> CreatePlan(BoundCTENode &node);
	unique_ptr<LogicalOperator> CreatePlan(BoundCTENode &node, unique_ptr<LogicalOperator> base);
	unique_ptr<LogicalOperator> CreatePlan(BoundSelectNode &statement);
	unique_ptr<LogicalOperator> CreatePlan(BoundSetOperationNode &node);
	unique_ptr<LogicalOperator> CreatePlan(BoundQueryNode &node);

	unique_ptr<BoundTableRef> BindJoin(Binder &parent, TableRef &ref);
	unique_ptr<BoundTableRef> Bind(BaseTableRef &ref);
	unique_ptr<BoundTableRef> Bind(JoinRef &ref);
	unique_ptr<BoundTableRef> Bind(SubqueryRef &ref, optional_ptr<CommonTableExpressionInfo> cte = nullptr);
	unique_ptr<BoundTableRef> Bind(TableFunctionRef &ref);
	unique_ptr<BoundTableRef> Bind(EmptyTableRef &ref);
	unique_ptr<BoundTableRef> Bind(DelimGetRef &ref);
	unique_ptr<BoundTableRef> Bind(ExpressionListRef &ref);
	unique_ptr<BoundTableRef> Bind(ColumnDataRef &ref);
	unique_ptr<BoundTableRef> Bind(PivotRef &expr);
	unique_ptr<BoundTableRef> Bind(ShowRef &ref);

	unique_ptr<SelectNode> BindPivot(PivotRef &expr, vector<unique_ptr<ParsedExpression>> all_columns);
	unique_ptr<SelectNode> BindUnpivot(Binder &child_binder, PivotRef &expr,
	                                   vector<unique_ptr<ParsedExpression>> all_columns,
	                                   unique_ptr<ParsedExpression> &where_clause);
	unique_ptr<BoundTableRef> BindBoundPivot(PivotRef &expr);
	void ExtractUnpivotEntries(Binder &child_binder, PivotColumnEntry &entry, vector<UnpivotEntry> &unpivot_entries);
	void ExtractUnpivotColumnName(ParsedExpression &expr, vector<string> &result);

	bool BindTableFunctionParameters(TableFunctionCatalogEntry &table_function,
	                                 vector<unique_ptr<ParsedExpression>> &expressions, vector<LogicalType> &arguments,
	                                 vector<Value> &parameters, named_parameter_map_t &named_parameters,
	                                 unique_ptr<BoundSubqueryRef> &subquery, ErrorData &error);
	void BindTableInTableOutFunction(vector<unique_ptr<ParsedExpression>> &expressions,
	                                 unique_ptr<BoundSubqueryRef> &subquery);
	unique_ptr<LogicalOperator> BindTableFunction(TableFunction &function, vector<Value> parameters);
	unique_ptr<LogicalOperator> BindTableFunctionInternal(TableFunction &table_function, const TableFunctionRef &ref,
	                                                      vector<Value> parameters,
	                                                      named_parameter_map_t named_parameters,
	                                                      vector<LogicalType> input_table_types,
	                                                      vector<string> input_table_names);

	unique_ptr<LogicalOperator> CreatePlan(BoundBaseTableRef &ref);
	unique_ptr<LogicalOperator> CreatePlan(BoundJoinRef &ref);
	unique_ptr<LogicalOperator> CreatePlan(BoundSubqueryRef &ref);
	unique_ptr<LogicalOperator> CreatePlan(BoundTableFunction &ref);
	unique_ptr<LogicalOperator> CreatePlan(BoundEmptyTableRef &ref);
	unique_ptr<LogicalOperator> CreatePlan(BoundExpressionListRef &ref);
	unique_ptr<LogicalOperator> CreatePlan(BoundColumnDataRef &ref);
	unique_ptr<LogicalOperator> CreatePlan(BoundCTERef &ref);
	unique_ptr<LogicalOperator> CreatePlan(BoundPivotRef &ref);
	unique_ptr<LogicalOperator> CreatePlan(BoundDelimGetRef &ref);

	BoundStatement BindCopyTo(CopyStatement &stmt, CopyToType copy_to_type);
	BoundStatement BindCopyFrom(CopyStatement &stmt);

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
	                                            vector<CorrelatedColumnInfo> &correlated_columns,
	                                            JoinType join_type = JoinType::INNER,
	                                            unique_ptr<Expression> condition = nullptr);

	unique_ptr<LogicalOperator> CastLogicalOperatorToTypes(vector<LogicalType> &source_types,
	                                                       vector<LogicalType> &target_types,
	                                                       unique_ptr<LogicalOperator> op);

	BindingAlias FindBinding(const string &using_column, const string &join_side);
	bool TryFindBinding(const string &using_column, const string &join_side, BindingAlias &result);

	void AddUsingBindingSet(unique_ptr<UsingColumnSet> set);
	BindingAlias RetrieveUsingBinding(Binder &current_binder, optional_ptr<UsingColumnSet> current_set,
	                                  const string &column_name, const string &join_side);

	void AddCTEMap(CommonTableExpressionMap &cte_map);

	void ExpandStarExpressions(vector<unique_ptr<ParsedExpression>> &select_list,
	                           vector<unique_ptr<ParsedExpression>> &new_select_list);
	void ExpandStarExpression(unique_ptr<ParsedExpression> expr, vector<unique_ptr<ParsedExpression>> &new_select_list);
	bool FindStarExpression(unique_ptr<ParsedExpression> &expr, StarExpression **star, bool is_root, bool in_columns);
	void ReplaceUnpackedStarExpression(unique_ptr<ParsedExpression> &expr,
	                                   vector<unique_ptr<ParsedExpression>> &replacements);
	void ReplaceStarExpression(unique_ptr<ParsedExpression> &expr, unique_ptr<ParsedExpression> &replacement);
	void BindWhereStarExpression(unique_ptr<ParsedExpression> &expr);

	//! If only a schema name is provided (e.g. "a.b") then figure out if "a" is a schema or a catalog name
	void BindSchemaOrCatalog(string &catalog_name, string &schema_name);
	const string BindCatalog(string &catalog_name);
	SchemaCatalogEntry &BindCreateSchema(CreateInfo &info);

	unique_ptr<BoundQueryNode> BindSelectNode(SelectNode &statement, unique_ptr<BoundTableRef> from_table);

	unique_ptr<LogicalOperator> BindCopyDatabaseSchema(Catalog &source_catalog, const string &target_database_name);
	unique_ptr<LogicalOperator> BindCopyDatabaseData(Catalog &source_catalog, const string &target_database_name);

	unique_ptr<BoundTableRef> BindShowQuery(ShowRef &ref);
	unique_ptr<BoundTableRef> BindShowTable(ShowRef &ref);
	unique_ptr<BoundTableRef> BindSummarize(ShowRef &ref);

	unique_ptr<LogicalOperator> UnionOperators(vector<unique_ptr<LogicalOperator>> nodes);

private:
	Binder(ClientContext &context, shared_ptr<Binder> parent, BinderType binder_type);
};

} // namespace duckdb
