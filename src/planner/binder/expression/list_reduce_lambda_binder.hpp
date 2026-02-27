//===----------------------------------------------------------------------===//
//                         DuckDB
//
// list_reduce_lambda_binder.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include <functional>
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_lambda_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

LogicalType ListReduceBindLambdaOverride(ClientContext &context, const vector<LogicalType> &function_child_types,
                                         const idx_t parameter_idx,
                                         optional_ptr<BindLambdaContext> bind_lambda_context);

using bind_lambda_expression_t =
    std::function<BindResult(LambdaExpression &, idx_t, const vector<LogicalType> &,
                             optional_ptr<bind_lambda_function_t>, optional_ptr<BindLambdaContext>)>;

struct ListReduceRebindResult {
	bool did_rebind = false;
	vector<LogicalType> capture_child_types;
	bind_lambda_function_t override_bind_lambda = nullptr;
	unique_ptr<BindLambdaContext> override_bind_lambda_context;
	LogicalType override_accumulator_type_storage;
	bool override_has_accumulator_type = false;
};

ListReduceRebindResult MaybeRebindListReduceLambda(ClientContext &context, idx_t depth,
                                                   const vector<LogicalType> &function_child_types,
                                                   bind_lambda_function_t bind_lambda_function,
                                                   BindResult &bind_lambda_result,
                                                   const bind_lambda_expression_t &bind_lambda_expression,
                                                   optional_ptr<BindLambdaContext> bind_lambda_context,
                                                   const unique_ptr<ParsedExpression> &lambda_expr_copy);

} // namespace duckdb
