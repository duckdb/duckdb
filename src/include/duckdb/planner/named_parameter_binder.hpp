//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/named_parameter_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/binder.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {

class NamedParameterBinder {
public:
    NamedParameterBinder(Binder &binder, QueryErrorContext error_context);
    Binder &binder;
    QueryErrorContext error_context;
public:
    void EvaluateInputParameters(vector<LogicalType> &arguments, vector<Value> &parameters, unordered_map<string, Value> &named_parameters, vector<unique_ptr<ParsedExpression>> &children, string func_type);
    void CheckNamedParameters(unordered_map<string, LogicalType> named_parameter_types, unordered_map<string, Value> named_parameter_values, string func_name);
};

} // namespace duckdb
