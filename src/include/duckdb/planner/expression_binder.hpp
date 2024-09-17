//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/stack_checker.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/parser/expression/lambdaref_expression.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/tokens.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/catalog/catalog_entry_retriever.hpp"
#include "duckdb/planner/expression/bound_lambda_expression.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/column_binding.hpp"

namespace duckdb {

class Binder;
class ClientContext;
class QueryNode;

class ScalarFunctionCatalogEntry;
class AggregateFunctionCatalogEntry;
class ScalarMacroCatalogEntry;
class CatalogEntry;
class SimpleFunction;

struct DummyBinding;
struct SelectBindState;

struct BoundColumnReferenceInfo {
	string name;
	optional_idx query_location;
};

struct BindResult {
	BindResult() {
	}
	explicit BindResult(const Exception &ex) : error(ex) {
	}
	explicit BindResult(const string &error_msg) : error(ExceptionType::BINDER, error_msg) {
	}
	explicit BindResult(ErrorData error) : error(std::move(error)) {
	}
	explicit BindResult(unique_ptr<Expression> expr) : expression(std::move(expr)) {
	}

	bool HasError() const {
		return error.HasError();
	}
	void SetError(const string &error_message) {
		error = ErrorData(ExceptionType::BINDER, error_message);
	}

	unique_ptr<Expression> expression;
	ErrorData error;
};

class ExpressionBinder {
	friend class StackChecker<ExpressionBinder>;

public:
	ExpressionBinder(Binder &binder, ClientContext &context, bool replace_binder = false);
	virtual ~ExpressionBinder();

	//! The target type that should result from the binder. If the result is not of this type, a cast to this type will
	//! be added. Defaults to INVALID.
	LogicalType target_type;

	optional_ptr<DummyBinding> macro_binding;
	optional_ptr<vector<DummyBinding>> lambda_bindings;

public:
	unique_ptr<Expression> Bind(unique_ptr<ParsedExpression> &expr, optional_ptr<LogicalType> result_type = nullptr,
	                            bool root_expression = true);

	//! Returns whether or not any columns have been bound by the expression binder
	bool HasBoundColumns() {
		return !bound_columns.empty();
	}
	const vector<BoundColumnReferenceInfo> &GetBoundColumns() {
		return bound_columns;
	}

	void SetCatalogLookupCallback(catalog_entry_callback_t callback);
	ErrorData Bind(unique_ptr<ParsedExpression> &expr, idx_t depth, bool root_expression = false);

	//! Returns the STRUCT_EXTRACT operator expression
	unique_ptr<ParsedExpression> CreateStructExtract(unique_ptr<ParsedExpression> base, const string &field_name);
	//! Returns a STRUCT_PACK function expression
	unique_ptr<ParsedExpression> CreateStructPack(ColumnRefExpression &col_ref);

	BindResult BindQualifiedColumnName(ColumnRefExpression &colref, const string &table_name);

	//! Returns a qualified column reference from a column name
	unique_ptr<ParsedExpression> QualifyColumnName(const string &column_name, ErrorData &error);
	//! Returns a qualified column reference from a column reference with column_names.size() > 2
	unique_ptr<ParsedExpression> QualifyColumnNameWithManyDots(ColumnRefExpression &col_ref, ErrorData &error);
	//! Returns a qualified column reference from a column reference
	unique_ptr<ParsedExpression> QualifyColumnName(ColumnRefExpression &col_ref, ErrorData &error);
	//! Enables special-handling of lambda parameters by tracking them in the lambda_params vector
	void QualifyColumnNamesInLambda(FunctionExpression &function, vector<unordered_set<string>> &lambda_params);
	//! Recursively qualifies the column references in the (children) of the expression. Passes on the
	//! within_function_expression state from outer expressions, or sets it
	void QualifyColumnNames(unique_ptr<ParsedExpression> &expr, vector<unordered_set<string>> &lambda_params,
	                        const bool within_function_expression = false);
	//! Entry point for qualifying the column references of the expression
	static void QualifyColumnNames(Binder &binder, unique_ptr<ParsedExpression> &expr);

	static bool PushCollation(ClientContext &context, unique_ptr<Expression> &source, const LogicalType &sql_type);
	static void TestCollation(ClientContext &context, const string &collation);

	BindResult BindCorrelatedColumns(unique_ptr<ParsedExpression> &expr, ErrorData error_message);

	void BindChild(unique_ptr<ParsedExpression> &expr, idx_t depth, ErrorData &error);
	static void ExtractCorrelatedExpressions(Binder &binder, Expression &expr);

	static bool ContainsNullType(const LogicalType &type);
	static LogicalType ExchangeNullType(const LogicalType &type);
	static bool ContainsType(const LogicalType &type, LogicalTypeId target);
	static LogicalType ExchangeType(const LogicalType &type, LogicalTypeId target, LogicalType new_type);

	virtual bool TryBindAlias(ColumnRefExpression &colref, bool root_expression, BindResult &result);
	virtual bool QualifyColumnAlias(const ColumnRefExpression &colref);

	//! Bind the given expression. Unlike Bind(), this does *not* mute the given ParsedExpression.
	//! Exposed to be used from sub-binders that aren't subclasses of ExpressionBinder.
	virtual BindResult BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth,
	                                  bool root_expression = false);

	//! FIXME: Generalise this for extensibility.
	//! Recursively replaces macro parameters with the provided input parameters.
	void ReplaceMacroParameters(unique_ptr<ParsedExpression> &expr, vector<unordered_set<string>> &lambda_params);
	//! Enables special-handling of lambda parameters during macro replacement by tracking them in the lambda_params
	//! vector.
	void ReplaceMacroParametersInLambda(FunctionExpression &function, vector<unordered_set<string>> &lambda_params);
	//! Recursively qualifies column references in ON CONFLICT DO UPDATE SET expressions.
	void DoUpdateSetQualify(unique_ptr<ParsedExpression> &expr, const string &table_name,
	                        vector<unordered_set<string>> &lambda_params);
	//! Enables special-handling of lambda parameters during ON CONFLICT TO UPDATE SET qualification by tracking them in
	//! the lambda_params vector.
	void DoUpdateSetQualifyInLambda(FunctionExpression &function, const string &table_name,
	                                vector<unordered_set<string>> &lambda_params);

	static LogicalType GetExpressionReturnType(const Expression &expr);

private:
	//! Current stack depth
	idx_t stack_depth = DConstants::INVALID_INDEX;

	void InitializeStackCheck();
	StackChecker<ExpressionBinder> StackCheck(const ParsedExpression &expr, idx_t extra_stack = 1);

protected:
	BindResult BindExpression(BetweenExpression &expr, idx_t depth);
	BindResult BindExpression(CaseExpression &expr, idx_t depth);
	BindResult BindExpression(CollateExpression &expr, idx_t depth);
	BindResult BindExpression(CastExpression &expr, idx_t depth);
	BindResult BindExpression(ColumnRefExpression &expr, idx_t depth, bool root_expression);
	BindResult BindExpression(LambdaRefExpression &expr, idx_t depth);
	BindResult BindExpression(ComparisonExpression &expr, idx_t depth);
	BindResult BindExpression(ConjunctionExpression &expr, idx_t depth);
	BindResult BindExpression(ConstantExpression &expr, idx_t depth);
	BindResult BindExpression(FunctionExpression &expr, idx_t depth, unique_ptr<ParsedExpression> &expr_ptr);
	BindResult BindExpression(LambdaExpression &expr, idx_t depth, const LogicalType &list_child_type,
	                          optional_ptr<bind_lambda_function_t> bind_lambda_function);
	BindResult BindExpression(OperatorExpression &expr, idx_t depth);
	BindResult BindExpression(ParameterExpression &expr, idx_t depth);
	BindResult BindExpression(SubqueryExpression &expr, idx_t depth);
	BindResult BindPositionalReference(unique_ptr<ParsedExpression> &expr, idx_t depth, bool root_expression);

	void TransformCapturedLambdaColumn(unique_ptr<Expression> &original, unique_ptr<Expression> &replacement,
	                                   BoundLambdaExpression &bound_lambda_expr,
	                                   const optional_ptr<bind_lambda_function_t> bind_lambda_function,
	                                   const LogicalType &list_child_type);
	void CaptureLambdaColumns(BoundLambdaExpression &bound_lambda_expr, unique_ptr<Expression> &expr,
	                          const optional_ptr<bind_lambda_function_t> bind_lambda_function,
	                          const LogicalType &list_child_type);

	virtual unique_ptr<ParsedExpression> GetSQLValueFunction(const string &column_name);

	LogicalType ResolveOperatorType(OperatorExpression &op, vector<unique_ptr<Expression>> &children);
	LogicalType ResolveCoalesceType(OperatorExpression &op, vector<unique_ptr<Expression>> &children);
	LogicalType ResolveNotType(OperatorExpression &op, vector<unique_ptr<Expression>> &children);

	BindResult BindUnsupportedExpression(ParsedExpression &expr, idx_t depth, const string &message);

protected:
	virtual BindResult BindGroupingFunction(OperatorExpression &op, idx_t depth);
	virtual BindResult BindFunction(FunctionExpression &expr, ScalarFunctionCatalogEntry &function, idx_t depth);
	virtual BindResult BindLambdaFunction(FunctionExpression &expr, ScalarFunctionCatalogEntry &function, idx_t depth);
	virtual BindResult BindAggregate(FunctionExpression &expr, AggregateFunctionCatalogEntry &function, idx_t depth);
	virtual BindResult BindUnnest(FunctionExpression &expr, idx_t depth, bool root_expression);
	virtual BindResult BindMacro(FunctionExpression &expr, ScalarMacroCatalogEntry &macro, idx_t depth,
	                             unique_ptr<ParsedExpression> &expr_ptr);
	void UnfoldMacroExpression(FunctionExpression &function, ScalarMacroCatalogEntry &macro_func,
	                           unique_ptr<ParsedExpression> &expr);

	virtual string UnsupportedAggregateMessage();
	virtual string UnsupportedUnnestMessage();
	optional_ptr<CatalogEntry> GetCatalogEntry(CatalogType type, const string &catalog, const string &schema,
	                                           const string &name, OnEntryNotFound on_entry_not_found,
	                                           QueryErrorContext &error_context);

	Binder &binder;
	ClientContext &context;
	optional_ptr<ExpressionBinder> stored_binder;
	vector<BoundColumnReferenceInfo> bound_columns;

	//! Returns true if the function name is an alias for the UNNEST function
	static bool IsUnnestFunction(const string &function_name);
	BindResult TryBindLambdaOrJson(FunctionExpression &function, idx_t depth, CatalogEntry &func);
	virtual void ThrowIfUnnestInLambda(const ColumnBinding &column_binding);
};

} // namespace duckdb
