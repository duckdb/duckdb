//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/scalar/string_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "function/scalar_function.hpp"

namespace re2 {
	class RE2;
}

namespace duckdb {

// case convert
void caseconvert_upper_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count,
                                BoundFunctionExpression &expr, Vector &result);
void caseconvert_lower_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count,
                                BoundFunctionExpression &expr, Vector &result);

bool caseconvert_matches_arguments(vector<SQLType> &arguments);
SQLType caseconvert_get_return_type(vector<SQLType> &arguments);

// concat
void concat_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                     Vector &result);
bool concat_matches_arguments(vector<SQLType> &arguments);
SQLType concat_get_return_type(vector<SQLType> &arguments);

// length
void length_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                     Vector &result);
bool length_matches_arguments(vector<SQLType> &arguments);
SQLType length_get_return_type(vector<SQLType> &arguments);

// regexp_matches
void regexp_matches_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count,
                             BoundFunctionExpression &expr, Vector &result);
bool regexp_matches_matches_arguments(vector<SQLType> &arguments);
SQLType regexp_matches_get_return_type(vector<SQLType> &arguments);
unique_ptr<FunctionData> regexp_matches_get_bind_function(BoundFunctionExpression &expr, ClientContext &context);

// regexp_replace
void regexp_replace_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count,
                             BoundFunctionExpression &expr, Vector &result);
bool regexp_replace_matches_arguments(vector<SQLType> &arguments);
SQLType regexp_replace_get_return_type(vector<SQLType> &arguments);

// substring
void substring_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                        Vector &result);
bool substring_matches_arguments(vector<SQLType> &arguments);
SQLType substring_get_return_type(vector<SQLType> &arguments);

struct RegexpMatchesBindData : public FunctionData {
    RegexpMatchesBindData(std::unique_ptr<re2::RE2> constant_pattern, string range_min, string range_max, bool range_success);
	~RegexpMatchesBindData();

    std::unique_ptr<re2::RE2> constant_pattern;
    string range_min, range_max;
    bool range_success;

    unique_ptr<FunctionData> Copy() override;
};

} // namespace duckdb
