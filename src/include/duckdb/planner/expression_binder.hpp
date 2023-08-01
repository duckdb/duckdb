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
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/tokens.hpp"
#include "duckdb/planner/expression.hpp"

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

struct BoundColumnReferenceInfo {
	string name;
	idx_t query_location;
};

struct BindResult {
	BindResult() {
	}
	explicit BindResult(string error) : error(error) {
	}
	explicit BindResult(unique_ptr<Expression> expr) : expression(std::move(expr)) {
	}

	bool HasError() {
		return !error.empty();
	}

	unique_ptr<Expression> expression;
	string error;
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

	string Bind(unique_ptr<ParsedExpression> &expr, idx_t depth, bool root_expression = false);

	unique_ptr<ParsedExpression> CreateStructExtract(unique_ptr<ParsedExpression> base, string field_name);
	unique_ptr<ParsedExpression> CreateStructPack(ColumnRefExpression &colref);
	BindResult BindQualifiedColumnName(ColumnRefExpression &colref, const string &table_name);

	unique_ptr<ParsedExpression> QualifyColumnName(const string &column_name, string &error_message);
	unique_ptr<ParsedExpression> QualifyColumnName(ColumnRefExpression &colref, string &error_message);

	// Bind table names to ColumnRefExpressions
	void QualifyColumnNames(unique_ptr<ParsedExpression> &expr);
	static void QualifyColumnNames(Binder &binder, unique_ptr<ParsedExpression> &expr);

	static unique_ptr<Expression> PushCollation(ClientContext &context, unique_ptr<Expression> source,
	                                            const string &collation, bool equality_only = false);
	static void TestCollation(ClientContext &context, const string &collation);

	bool BindCorrelatedColumns(unique_ptr<ParsedExpression> &expr);

	void BindChild(unique_ptr<ParsedExpression> &expr, idx_t depth, string &error);
	static void ExtractCorrelatedExpressions(Binder &binder, Expression &expr);

	static bool ContainsNullType(const LogicalType &type);
	static LogicalType ExchangeNullType(const LogicalType &type);
	static bool ContainsType(const LogicalType &type, LogicalTypeId target);
	static LogicalType ExchangeType(const LogicalType &type, LogicalTypeId target, LogicalType new_type);

	virtual bool QualifyColumnAlias(const ColumnRefExpression &colref);

	//! Bind the given expresion. Unlike Bind(), this does *not* mute the given ParsedExpression.
	//! Exposed to be used from sub-binders that aren't subclasses of ExpressionBinder.
	virtual BindResult BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth,
	                                  bool root_expression = false);

	void ReplaceMacroParametersRecursive(unique_ptr<ParsedExpression> &expr);

private:
	//! Maximum stack depth
	static constexpr const idx_t MAXIMUM_STACK_DEPTH = 128;
	//! Current stack depth
	idx_t stack_depth = DConstants::INVALID_INDEX;

	void InitializeStackCheck();
	StackChecker<ExpressionBinder> StackCheck(const ParsedExpression &expr, idx_t extra_stack = 1);

protected:
	BindResult BindExpression(BetweenExpression &expr, idx_t depth);
	BindResult BindExpression(CaseExpression &expr, idx_t depth);
	BindResult BindExpression(CollateExpression &expr, idx_t depth);
	BindResult BindExpression(CastExpression &expr, idx_t depth);
	BindResult BindExpression(ColumnRefExpression &expr, idx_t depth);
	BindResult BindExpression(ComparisonExpression &expr, idx_t depth);
	BindResult BindExpression(ConjunctionExpression &expr, idx_t depth);
	BindResult BindExpression(ConstantExpression &expr, idx_t depth);
	BindResult BindExpression(FunctionExpression &expr, idx_t depth, unique_ptr<ParsedExpression> &expr_ptr);
	BindResult BindExpression(LambdaExpression &expr, idx_t depth, const bool is_lambda,
	                          const LogicalType &list_child_type);
	BindResult BindExpression(OperatorExpression &expr, idx_t depth);
	BindResult BindExpression(ParameterExpression &expr, idx_t depth);
	BindResult BindExpression(SubqueryExpression &expr, idx_t depth);
	BindResult BindPositionalReference(unique_ptr<ParsedExpression> &expr, idx_t depth, bool root_expression);

	void TransformCapturedLambdaColumn(unique_ptr<Expression> &original, unique_ptr<Expression> &replacement,
	                                   vector<unique_ptr<Expression>> &captures, LogicalType &list_child_type);
	void CaptureLambdaColumns(vector<unique_ptr<Expression>> &captures, LogicalType &list_child_type,
	                          unique_ptr<Expression> &expr);

	static unique_ptr<ParsedExpression> GetSQLValueFunction(const string &column_name);

protected:
	virtual BindResult BindGroupingFunction(OperatorExpression &op, idx_t depth);
	virtual BindResult BindFunction(FunctionExpression &expr, ScalarFunctionCatalogEntry &function, idx_t depth);
	virtual BindResult BindLambdaFunction(FunctionExpression &expr, ScalarFunctionCatalogEntry &function, idx_t depth);
	virtual BindResult BindAggregate(FunctionExpression &expr, AggregateFunctionCatalogEntry &function, idx_t depth);
	virtual BindResult BindUnnest(FunctionExpression &expr, idx_t depth, bool root_expression);
	virtual BindResult BindMacro(FunctionExpression &expr, ScalarMacroCatalogEntry &macro, idx_t depth,
	                             unique_ptr<ParsedExpression> &expr_ptr);

	virtual string UnsupportedAggregateMessage();
	virtual string UnsupportedUnnestMessage();

	Binder &binder;
	ClientContext &context;
	optional_ptr<ExpressionBinder> stored_binder;
	vector<BoundColumnReferenceInfo> bound_columns;
};

} // namespace duckdb
