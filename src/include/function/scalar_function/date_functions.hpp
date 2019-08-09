//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/scalar_function/date_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "function/scalar_function.hpp"

namespace duckdb {

void age_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                  Vector &result);
bool age_matches_arguments(vector<SQLType> &arguments);
SQLType age_get_return_type(vector<SQLType> &arguments);

void year_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                   Vector &result);
bool year_matches_arguments(vector<SQLType> &arguments);
SQLType year_get_return_type(vector<SQLType> &arguments);

void date_part_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                        Vector &result);
bool date_part_matches_arguments(vector<SQLType> &arguments);
SQLType date_part_get_return_type(vector<SQLType> &arguments);

} // namespace duckdb
