//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/column_qualifier.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/binder.hpp"

namespace duckdb {
class ColumnAliasBinder;
class HavingBinder;

class ColumnQualifier {
public:
	explicit ColumnQualifier(Binder &binder, optional_ptr<vector<DummyBinding>> lambda_bindings = nullptr,
	                         optional_ptr<ColumnAliasBinder> alias_binder = nullptr,
	                         optional_ptr<HavingBinder> having_binder = nullptr);
	virtual ~ColumnQualifier() = default;

	//! Returns the STRUCT_EXTRACT operator expression
	unique_ptr<ParsedExpression> CreateStructExtract(unique_ptr<ParsedExpression> base, const Identifier &field_name);
	//! Returns a STRUCT_PACK function expression
	unique_ptr<ParsedExpression> CreateStructPack(const ColumnRefExpression &col_ref);

	//! Returns a qualified column reference from a column name
	unique_ptr<ParsedExpression> QualifyColumnName(const ParsedExpression &expr, const Identifier &column_name,
	                                               ErrorData &error);
	//! Returns a qualified column reference from a column reference with column_names.size() > 2
	unique_ptr<ParsedExpression> QualifyColumnNameWithManyDots(const ColumnRefExpression &col_ref, ErrorData &error);
	//! Returns a qualified column reference from a column reference
	unique_ptr<ParsedExpression> QualifyColumnName(const ColumnRefExpression &col_ref, ErrorData &error);
	//! Enables special-handling of lambda parameters by tracking them in the lambda_params vector
	void QualifyColumnNamesInLambda(FunctionExpression &function, vector<identifier_set_t> &lambda_params);
	//! Recursively qualifies the column references in the (children) of the expression. Passes on the
	//! within_function_expression state from outer expressions, or sets it
	void QualifyColumnNames(unique_ptr<ParsedExpression> &expr, vector<identifier_set_t> &lambda_params,
	                        const bool within_function_expression = false);

	//! Look up the function a call refers to. A call that resolves to a different form than it is written in, e.g. the
	//! dot-call "x.lower()" that is "lower(x)", is returned in that form in `qualified`
	optional_ptr<CatalogEntry> QualifyFunction(const FunctionExpression &function,
	                                           unique_ptr<FunctionExpression> &qualified);
	//! Look up the function a call refers to, replacing the call with the form it resolves to
	optional_ptr<CatalogEntry> QualifyFunction(unique_ptr<ParsedExpression> &expr);

private:
	Binder &binder;
	optional_ptr<vector<DummyBinding>> lambda_bindings;
	optional_ptr<ColumnAliasBinder> alias_binder;
	optional_ptr<HavingBinder> having_binder;

private:
	unique_ptr<ParsedExpression> QualifyColumnNameWithManyDotsInternal(const ColumnRefExpression &col_ref,
	                                                                   ErrorData &error, idx_t &struct_extract_start);
	//! Whether the given (possibly nested) table qualification matches a binding in the current bind context
	bool TableBindingExists(const BindingAlias &alias);
	//! Whether any binding in the current bind context has the given table name (ignoring catalog/schema)
	bool TableNameExists(const Identifier &table_name);

	unique_ptr<ParsedExpression> QualifyColumnNameInternal(const ColumnRefExpression &col_ref, ErrorData &error);
};

} // namespace duckdb
