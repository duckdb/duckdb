//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_map.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/tokens.hpp"
#include "duckdb/planner/bind_context.hpp"
#include "duckdb/planner/bound_tokens.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/bound_statement.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/parser/result_modifier.hpp"
#include "duckdb/common/enums/statement_type.hpp"

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

struct CreateInfo;
struct BoundCreateTableInfo;
struct BoundCreateFunctionInfo;
struct CommonTableExpressionInfo;

enum class BindingMode : uint8_t { STANDARD_BINDING, EXTRACT_NAMES };

struct CorrelatedColumnInfo {
	ColumnBinding binding;
	LogicalType type;
	string name;
	idx_t depth;

	explicit CorrelatedColumnInfo(BoundColumnRefExpression &expr)
	    : binding(expr.binding), type(expr.return_type), name(expr.GetName()), depth(expr.depth) {
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
	friend class SelectBinder;
	friend class RecursiveSubqueryPlanner;

public:
	static shared_ptr<Binder> CreateBinder(ClientContext &context, Binder *parent = nullptr, bool inherit_ctes = true);

	//! The client context
	ClientContext &context;
	//! A mapping of names to common table expressions
	case_insensitive_map_t<CommonTableExpressionInfo *> CTE_bindings;
	//! The CTEs that have already been bound
	unordered_set<CommonTableExpressionInfo *> bound_ctes;
	//! The bind context
	BindContext bind_context;
	//! The set of correlated columns bound by this binder (FIXME: this should probably be an unordered_set and not a
	//! vector)
	vector<CorrelatedColumnInfo> correlated_columns;
	//! The set of parameter expressions bound by this binder
	vector<BoundParameterExpression *> *parameters;
	//! The types of the prepared statement parameters, if any
	vector<LogicalType> *parameter_types;
	//! Statement properties
	StatementProperties properties;
	//! The alias for the currently processing subquery, if it exists
	string alias;
	//! Macro parameter bindings (if any)
	MacroBinding *macro_binding = nullptr;

public:
	BoundStatement Bind(SQLStatement &statement);
	BoundStatement Bind(QueryNode &node);

	unique_ptr<BoundCreateTableInfo> BindCreateTableInfo(unique_ptr<CreateInfo> info);
	void BindCreateViewInfo(CreateViewInfo &base);
	SchemaCatalogEntry *BindSchema(CreateInfo &info);
	SchemaCatalogEntry *BindCreateFunctionInfo(CreateInfo &info);

	//! Check usage, and cast named parameters to their types
	static void BindNamedParameters(named_parameter_type_map_t &types, named_parameter_map_t &values,
	                                QueryErrorContext &error_context, string &func_name);

	unique_ptr<BoundTableRef> Bind(TableRef &ref);
	unique_ptr<LogicalOperator> CreatePlan(BoundTableRef &ref);

	//! Generates an unused index for a table
	idx_t GenerateTableIndex();

	//! Add a common table expression to the binder
	void AddCTE(const string &name, CommonTableExpressionInfo *cte);
	//! Find a common table expression by name; returns nullptr if none exists
	CommonTableExpressionInfo *FindCTE(const string &name, bool skip = false);

	bool CTEIsAlreadyBound(CommonTableExpressionInfo *cte);

	//! Add the view to the set of currently bound views - used for detecting recursive view definitions
	void AddBoundView(ViewCatalogEntry *view);

	void PushExpressionBinder(ExpressionBinder *binder);
	void PopExpressionBinder();
	void SetActiveBinder(ExpressionBinder *binder);
	ExpressionBinder *GetActiveBinder();
	bool HasActiveBinder();

	vector<ExpressionBinder *> &GetActiveBinders();

	void MergeCorrelatedColumns(vector<CorrelatedColumnInfo> &other);
	//! Add a correlated column to this binder (if it does not exist)
	void AddCorrelatedColumn(const CorrelatedColumnInfo &info);

	string FormatError(ParsedExpression &expr_context, const string &message);
	string FormatError(TableRef &ref_context, const string &message);

	string FormatErrorRecursive(idx_t query_location, const string &message, vector<ExceptionFormatValue> &values);
	template <class T, typename... Args>
	string FormatErrorRecursive(idx_t query_location, const string &msg, vector<ExceptionFormatValue> &values, T param,
	                            Args... params) {
		values.push_back(ExceptionFormatValue::CreateFormatValue<T>(param));
		return FormatErrorRecursive(query_location, msg, values, params...);
	}

	template <typename... Args>
	string FormatError(idx_t query_location, const string &msg, Args... params) {
		vector<ExceptionFormatValue> values;
		return FormatErrorRecursive(query_location, msg, values, params...);
	}

	static void BindLogicalType(ClientContext &context, LogicalType &type, const string &schema = "");

	bool HasMatchingBinding(const string &table_name, const string &column_name, string &error_message);
	bool HasMatchingBinding(const string &schema_name, const string &table_name, const string &column_name,
	                        string &error_message);

	void SetBindingMode(BindingMode mode);
	BindingMode GetBindingMode();
	void AddTableName(string table_name);
	const unordered_set<string> &GetTableNames();

private:
	//! The parent binder (if any)
	shared_ptr<Binder> parent;
	//! The vector of active binders
	vector<ExpressionBinder *> active_binders;
	//! The count of bound_tables
	idx_t bound_tables;
	//! Whether or not the binder has any unplanned subqueries that still need to be planned
	bool has_unplanned_subqueries = false;
	//! Whether or not subqueries should be planned already
	bool plan_subquery = true;
	//! Whether CTEs should reference the parent binder (if it exists)
	bool inherit_ctes = true;
	//! Whether or not the binder can contain NULLs as the root of expressions
	bool can_contain_nulls = false;
	//! The root statement of the query that is currently being parsed
	SQLStatement *root_statement = nullptr;
	//! Binding mode
	BindingMode mode = BindingMode::STANDARD_BINDING;
	//! Table names extracted for BindingMode::EXTRACT_NAMES
	unordered_set<string> table_names;
	//! The set of bound views
	unordered_set<ViewCatalogEntry *> bound_views;

private:
	//! Bind the default values of the columns of a table
	void BindDefaultValues(vector<ColumnDefinition> &columns, vector<unique_ptr<Expression>> &bound_defaults);
	//! Bind a limit value (LIMIT or OFFSET)
	unique_ptr<Expression> BindDelimiter(ClientContext &context, OrderBinder &order_binder,
	                                     unique_ptr<ParsedExpression> delimiter, const LogicalType &type,
	                                     Value &delimiter_value);

	//! Move correlated expressions from the child binder to this binder
	void MoveCorrelatedExpressions(Binder &other);

	BoundStatement Bind(SelectStatement &stmt);
	BoundStatement Bind(InsertStatement &stmt);
	BoundStatement Bind(CopyStatement &stmt);
	BoundStatement Bind(DeleteStatement &stmt);
	BoundStatement Bind(UpdateStatement &stmt);
	BoundStatement Bind(CreateStatement &stmt);
	BoundStatement Bind(DropStatement &stmt);
	BoundStatement Bind(AlterStatement &stmt);
	BoundStatement Bind(TransactionStatement &stmt);
	BoundStatement Bind(PragmaStatement &stmt);
	BoundStatement Bind(ExplainStatement &stmt);
	BoundStatement Bind(VacuumStatement &stmt);
	BoundStatement Bind(RelationStatement &stmt);
	BoundStatement Bind(ShowStatement &stmt);
	BoundStatement Bind(CallStatement &stmt);
	BoundStatement Bind(ExportStatement &stmt);
	BoundStatement Bind(SetStatement &stmt);
	BoundStatement Bind(LoadStatement &stmt);
	BoundStatement BindReturning(vector<unique_ptr<ParsedExpression>> returning_list, TableCatalogEntry *table,
	                             idx_t update_table_index, unique_ptr<LogicalOperator> child_operator,
	                             BoundStatement result);

    void BindTable(CatalogType catalog_type, BoundStatement &result, CreateStatement &stmt);

	unique_ptr<QueryNode> BindTableMacro(FunctionExpression &function, TableMacroCatalogEntry *macro_func, idx_t depth);

	unique_ptr<BoundQueryNode> BindNode(SelectNode &node);
	unique_ptr<BoundQueryNode> BindNode(SetOperationNode &node);
	unique_ptr<BoundQueryNode> BindNode(RecursiveCTENode &node);
	unique_ptr<BoundQueryNode> BindNode(QueryNode &node);

	unique_ptr<LogicalOperator> VisitQueryNode(BoundQueryNode &node, unique_ptr<LogicalOperator> root);
	unique_ptr<LogicalOperator> CreatePlan(BoundRecursiveCTENode &node);
	unique_ptr<LogicalOperator> CreatePlan(BoundSelectNode &statement);
	unique_ptr<LogicalOperator> CreatePlan(BoundSetOperationNode &node);
	unique_ptr<LogicalOperator> CreatePlan(BoundQueryNode &node);

	unique_ptr<BoundTableRef> Bind(BaseTableRef &ref);
	unique_ptr<BoundTableRef> Bind(CrossProductRef &ref);
	unique_ptr<BoundTableRef> Bind(JoinRef &ref);
	unique_ptr<BoundTableRef> Bind(SubqueryRef &ref, CommonTableExpressionInfo *cte = nullptr);
	unique_ptr<BoundTableRef> Bind(TableFunctionRef &ref);
	unique_ptr<BoundTableRef> Bind(EmptyTableRef &ref);
	unique_ptr<BoundTableRef> Bind(ExpressionListRef &ref);

	bool BindTableFunctionParameters(TableFunctionCatalogEntry &table_function,
	                                 vector<unique_ptr<ParsedExpression>> &expressions, vector<LogicalType> &arguments,
	                                 vector<Value> &parameters, named_parameter_map_t &named_parameters,
	                                 unique_ptr<BoundSubqueryRef> &subquery, string &error);
	bool BindTableInTableOutFunction(vector<unique_ptr<ParsedExpression>> &expressions,
	                                 unique_ptr<BoundSubqueryRef> &subquery, string &error);

	unique_ptr<LogicalOperator> CreatePlan(BoundBaseTableRef &ref);
	unique_ptr<LogicalOperator> CreatePlan(BoundCrossProductRef &ref);
	unique_ptr<LogicalOperator> CreatePlan(BoundJoinRef &ref);
	unique_ptr<LogicalOperator> CreatePlan(BoundSubqueryRef &ref);
	unique_ptr<LogicalOperator> CreatePlan(BoundTableFunction &ref);
	unique_ptr<LogicalOperator> CreatePlan(BoundEmptyTableRef &ref);
	unique_ptr<LogicalOperator> CreatePlan(BoundExpressionListRef &ref);
	unique_ptr<LogicalOperator> CreatePlan(BoundCTERef &ref);

	BoundStatement BindCopyTo(CopyStatement &stmt);
	BoundStatement BindCopyFrom(CopyStatement &stmt);

	void BindModifiers(OrderBinder &order_binder, QueryNode &statement, BoundQueryNode &result);
	void BindModifierTypes(BoundQueryNode &result, const vector<LogicalType> &sql_types, idx_t projection_index);

	BoundStatement BindSummarize(ShowStatement &stmt);
	unique_ptr<BoundResultModifier> BindLimit(OrderBinder &order_binder, LimitModifier &limit_mod);
	unique_ptr<BoundResultModifier> BindLimitPercent(OrderBinder &order_binder, LimitPercentModifier &limit_mod);
	unique_ptr<Expression> BindOrderExpression(OrderBinder &order_binder, unique_ptr<ParsedExpression> expr);

	unique_ptr<LogicalOperator> PlanFilter(unique_ptr<Expression> condition, unique_ptr<LogicalOperator> root);

	void PlanSubqueries(unique_ptr<Expression> *expr, unique_ptr<LogicalOperator> *root);
	unique_ptr<Expression> PlanSubquery(BoundSubqueryExpression &expr, unique_ptr<LogicalOperator> &root);

	unique_ptr<LogicalOperator> CastLogicalOperatorToTypes(vector<LogicalType> &source_types,
	                                                       vector<LogicalType> &target_types,
	                                                       unique_ptr<LogicalOperator> op);

	string FindBinding(const string &using_column, const string &join_side);
	bool TryFindBinding(const string &using_column, const string &join_side, string &result);

	void AddUsingBindingSet(unique_ptr<UsingColumnSet> set);
	string RetrieveUsingBinding(Binder &current_binder, UsingColumnSet *current_set, const string &column_name,
	                            const string &join_side, UsingColumnSet *new_set);

public:
	// This should really be a private constructor, but make_shared does not allow it...
	// If you are thinking about calling this, you should probably call Binder::CreateBinder
	Binder(bool I_know_what_I_am_doing, ClientContext &context, shared_ptr<Binder> parent, bool inherit_ctes);
};

} // namespace duckdb
