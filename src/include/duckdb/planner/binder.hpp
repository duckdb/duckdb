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
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/result_modifier.hpp"
#include "duckdb/parser/tokens.hpp"
#include "duckdb/planner/bind_context.hpp"
#include "duckdb/planner/bound_statement.hpp"
#include "duckdb/planner/bound_tokens.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/common/reference_map.hpp"

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

class ColumnList;
class ExternalDependency;
class TableFunction;
class TableStorageInfo;

struct CreateInfo;
struct BoundCreateTableInfo;
struct BoundCreateFunctionInfo;
struct CommonTableExpressionInfo;
struct BoundParameterMap;

enum class BindingMode : uint8_t { STANDARD_BINDING, EXTRACT_NAMES };

struct CorrelatedColumnInfo {
	ColumnBinding binding;
	LogicalType type;
	string name;
	idx_t depth;

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
class Binder : public std::enable_shared_from_this<Binder> {
	friend class ExpressionBinder;
	friend class RecursiveDependentJoinPlanner;

public:
	DUCKDB_API static shared_ptr<Binder> CreateBinder(ClientContext &context, optional_ptr<Binder> parent = nullptr,
	                                                  bool inherit_ctes = true);

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
	//! Statement properties
	StatementProperties properties;
	//! The alias for the currently processing subquery, if it exists
	string alias;
	//! Macro parameter bindings (if any)
	optional_ptr<DummyBinding> macro_binding;
	//! The intermediate lambda bindings to bind nested lambdas (if any)
	optional_ptr<vector<DummyBinding>> lambda_bindings;

public:
	DUCKDB_API BoundStatement Bind(SQLStatement &statement);
	DUCKDB_API BoundStatement Bind(QueryNode &node);

	unique_ptr<BoundCreateTableInfo> BindCreateTableInfo(unique_ptr<CreateInfo> info);
	unique_ptr<BoundCreateTableInfo> BindCreateTableInfo(unique_ptr<CreateInfo> info, SchemaCatalogEntry &schema);

	vector<unique_ptr<Expression>> BindCreateIndexExpressions(TableCatalogEntry &table, CreateIndexInfo &info);

	void BindCreateViewInfo(CreateViewInfo &base);
	SchemaCatalogEntry &BindSchema(CreateInfo &info);
	SchemaCatalogEntry &BindCreateFunctionInfo(CreateInfo &info);

	//! Check usage, and cast named parameters to their types
	static void BindNamedParameters(named_parameter_type_map_t &types, named_parameter_map_t &values,
	                                QueryErrorContext &error_context, string &func_name);

	unique_ptr<BoundTableRef> Bind(TableRef &ref);
	unique_ptr<LogicalOperator> CreatePlan(BoundTableRef &ref);

	//! Generates an unused index for a table
	idx_t GenerateTableIndex();

	//! Add a common table expression to the binder
	void AddCTE(const string &name, CommonTableExpressionInfo &cte);
	//! Find a common table expression by name; returns nullptr if none exists
	optional_ptr<CommonTableExpressionInfo> FindCTE(const string &name, bool skip = false);

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

	string FormatError(ParsedExpression &expr_context, const string &message);
	string FormatError(TableRef &ref_context, const string &message);

	string FormatErrorRecursive(idx_t query_location, const string &message, vector<ExceptionFormatValue> &values);
	template <class T, typename... ARGS>
	string FormatErrorRecursive(idx_t query_location, const string &msg, vector<ExceptionFormatValue> &values, T param,
	                            ARGS... params) {
		values.push_back(ExceptionFormatValue::CreateFormatValue<T>(param));
		return FormatErrorRecursive(query_location, msg, values, params...);
	}

	template <typename... ARGS>
	string FormatError(idx_t query_location, const string &msg, ARGS... params) {
		vector<ExceptionFormatValue> values;
		return FormatErrorRecursive(query_location, msg, values, params...);
	}

	unique_ptr<LogicalOperator> BindUpdateSet(LogicalOperator &op, unique_ptr<LogicalOperator> root,
	                                          UpdateSetInfo &set_info, TableCatalogEntry &table,
	                                          vector<PhysicalIndex> &columns);
	void BindDoUpdateSetExpressions(const string &table_alias, LogicalInsert &insert, UpdateSetInfo &set_info,
	                                TableCatalogEntry &table, TableStorageInfo &storage_info);
	void BindOnConflictClause(LogicalInsert &insert, TableCatalogEntry &table, InsertStatement &stmt);

	static void BindSchemaOrCatalog(ClientContext &context, string &catalog, string &schema);
	static void BindLogicalType(ClientContext &context, LogicalType &type, optional_ptr<Catalog> catalog = nullptr,
	                            const string &schema = INVALID_SCHEMA);

	bool HasMatchingBinding(const string &table_name, const string &column_name, string &error_message);
	bool HasMatchingBinding(const string &schema_name, const string &table_name, const string &column_name,
	                        string &error_message);
	bool HasMatchingBinding(const string &catalog_name, const string &schema_name, const string &table_name,
	                        const string &column_name, string &error_message);

	void SetBindingMode(BindingMode mode);
	BindingMode GetBindingMode();
	void AddTableName(string table_name);
	const unordered_set<string> &GetTableNames();
	optional_ptr<SQLStatement> GetRootStatement() {
		return root_statement;
	}

	void SetCanContainNulls(bool can_contain_nulls);

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
	//! Whether CTEs should reference the parent binder (if it exists)
	bool inherit_ctes = true;
	//! Whether or not the binder can contain NULLs as the root of expressions
	bool can_contain_nulls = false;
	//! The root statement of the query that is currently being parsed
	optional_ptr<SQLStatement> root_statement;
	//! Binding mode
	BindingMode mode = BindingMode::STANDARD_BINDING;
	//! Table names extracted for BindingMode::EXTRACT_NAMES
	unordered_set<string> table_names;
	//! The set of bound views
	reference_set_t<ViewCatalogEntry> bound_views;

private:
	//! Get the root binder (binder with no parent)
	Binder *GetRootBinder();
	//! Determine the depth of the binder
	idx_t GetBinderDepth() const;
	//! Bind the expressions of generated columns to check for errors
	void BindGeneratedColumns(BoundCreateTableInfo &info);
	//! Bind the default values of the columns of a table
	void BindDefaultValues(const ColumnList &columns, vector<unique_ptr<Expression>> &bound_defaults);
	//! Bind a limit value (LIMIT or OFFSET)
	unique_ptr<Expression> BindDelimiter(ClientContext &context, OrderBinder &order_binder,
	                                     unique_ptr<ParsedExpression> delimiter, const LogicalType &type,
	                                     Value &delimiter_value);

	//! Move correlated expressions from the child binder to this binder
	void MoveCorrelatedExpressions(Binder &other);

	//! Tries to bind the table name with replacement scans
	unique_ptr<BoundTableRef> BindWithReplacementScan(ClientContext &context, const string &table_name,
	                                                  BaseTableRef &ref);

	BoundStatement Bind(SelectStatement &stmt);
	BoundStatement Bind(InsertStatement &stmt);
	BoundStatement Bind(CopyStatement &stmt);
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
	BoundStatement Bind(ShowStatement &stmt);
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

	BoundStatement BindReturning(vector<unique_ptr<ParsedExpression>> returning_list, TableCatalogEntry &table,
	                             const string &alias, idx_t update_table_index,
	                             unique_ptr<LogicalOperator> child_operator, BoundStatement result);

	unique_ptr<QueryNode> BindTableMacro(FunctionExpression &function, TableMacroCatalogEntry &macro_func, idx_t depth);

	unique_ptr<BoundQueryNode> BindNode(SelectNode &node);
	unique_ptr<BoundQueryNode> BindNode(SetOperationNode &node);
	unique_ptr<BoundQueryNode> BindNode(RecursiveCTENode &node);
	unique_ptr<BoundQueryNode> BindNode(CTENode &node);
	unique_ptr<BoundQueryNode> BindNode(QueryNode &node);

	unique_ptr<LogicalOperator> VisitQueryNode(BoundQueryNode &node, unique_ptr<LogicalOperator> root);
	unique_ptr<LogicalOperator> CreatePlan(BoundRecursiveCTENode &node);
	unique_ptr<LogicalOperator> CreatePlan(BoundCTENode &node);
	unique_ptr<LogicalOperator> CreatePlan(BoundSelectNode &statement);
	unique_ptr<LogicalOperator> CreatePlan(BoundSetOperationNode &node);
	unique_ptr<LogicalOperator> CreatePlan(BoundQueryNode &node);

	unique_ptr<BoundTableRef> Bind(BaseTableRef &ref);
	unique_ptr<BoundTableRef> Bind(JoinRef &ref);
	unique_ptr<BoundTableRef> Bind(SubqueryRef &ref, optional_ptr<CommonTableExpressionInfo> cte = nullptr);
	unique_ptr<BoundTableRef> Bind(TableFunctionRef &ref);
	unique_ptr<BoundTableRef> Bind(EmptyTableRef &ref);
	unique_ptr<BoundTableRef> Bind(ExpressionListRef &ref);
	unique_ptr<BoundTableRef> Bind(PivotRef &expr);

	unique_ptr<SelectNode> BindPivot(PivotRef &expr, vector<unique_ptr<ParsedExpression>> all_columns);
	unique_ptr<SelectNode> BindUnpivot(Binder &child_binder, PivotRef &expr,
	                                   vector<unique_ptr<ParsedExpression>> all_columns,
	                                   unique_ptr<ParsedExpression> &where_clause);
	unique_ptr<BoundTableRef> BindBoundPivot(PivotRef &expr);

	bool BindTableFunctionParameters(TableFunctionCatalogEntry &table_function,
	                                 vector<unique_ptr<ParsedExpression>> &expressions, vector<LogicalType> &arguments,
	                                 vector<Value> &parameters, named_parameter_map_t &named_parameters,
	                                 unique_ptr<BoundSubqueryRef> &subquery, string &error);
	bool BindTableInTableOutFunction(vector<unique_ptr<ParsedExpression>> &expressions,
	                                 unique_ptr<BoundSubqueryRef> &subquery, string &error);
	unique_ptr<LogicalOperator> BindTableFunction(TableFunction &function, vector<Value> parameters);
	unique_ptr<LogicalOperator>
	BindTableFunctionInternal(TableFunction &table_function, const string &function_name, vector<Value> parameters,
	                          named_parameter_map_t named_parameters, vector<LogicalType> input_table_types,
	                          vector<string> input_table_names, const vector<string> &column_name_alias,
	                          unique_ptr<ExternalDependency> external_dependency);

	unique_ptr<LogicalOperator> CreatePlan(BoundBaseTableRef &ref);
	unique_ptr<LogicalOperator> CreatePlan(BoundJoinRef &ref);
	unique_ptr<LogicalOperator> CreatePlan(BoundSubqueryRef &ref);
	unique_ptr<LogicalOperator> CreatePlan(BoundTableFunction &ref);
	unique_ptr<LogicalOperator> CreatePlan(BoundEmptyTableRef &ref);
	unique_ptr<LogicalOperator> CreatePlan(BoundExpressionListRef &ref);
	unique_ptr<LogicalOperator> CreatePlan(BoundCTERef &ref);
	unique_ptr<LogicalOperator> CreatePlan(BoundPivotRef &ref);

	BoundStatement BindCopyTo(CopyStatement &stmt);
	BoundStatement BindCopyFrom(CopyStatement &stmt);

	void BindModifiers(OrderBinder &order_binder, QueryNode &statement, BoundQueryNode &result);
	void BindModifierTypes(BoundQueryNode &result, const vector<LogicalType> &sql_types, idx_t projection_index);

	BoundStatement BindSummarize(ShowStatement &stmt);
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

	string FindBinding(const string &using_column, const string &join_side);
	bool TryFindBinding(const string &using_column, const string &join_side, string &result);

	void AddUsingBindingSet(unique_ptr<UsingColumnSet> set);
	string RetrieveUsingBinding(Binder &current_binder, optional_ptr<UsingColumnSet> current_set,
	                            const string &column_name, const string &join_side);

	void AddCTEMap(CommonTableExpressionMap &cte_map);

	void ExpandStarExpressions(vector<unique_ptr<ParsedExpression>> &select_list,
	                           vector<unique_ptr<ParsedExpression>> &new_select_list);
	void ExpandStarExpression(unique_ptr<ParsedExpression> expr, vector<unique_ptr<ParsedExpression>> &new_select_list);
	bool FindStarExpression(unique_ptr<ParsedExpression> &expr, StarExpression **star, bool is_root, bool in_columns);
	void ReplaceStarExpression(unique_ptr<ParsedExpression> &expr, unique_ptr<ParsedExpression> &replacement);
	void BindWhereStarExpression(unique_ptr<ParsedExpression> &expr);

	//! If only a schema name is provided (e.g. "a.b") then figure out if "a" is a schema or a catalog name
	void BindSchemaOrCatalog(string &catalog_name, string &schema_name);
	SchemaCatalogEntry &BindCreateSchema(CreateInfo &info);

	unique_ptr<BoundQueryNode> BindSelectNode(SelectNode &statement, unique_ptr<BoundTableRef> from_table);

public:
	// This should really be a private constructor, but make_shared does not allow it...
	// If you are thinking about calling this, you should probably call Binder::CreateBinder
	Binder(bool i_know_what_i_am_doing, ClientContext &context, shared_ptr<Binder> parent, bool inherit_ctes);
};

} // namespace duckdb
