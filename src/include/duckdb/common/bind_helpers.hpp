//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/bind_helpers.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/common/vector.hpp"
#include "duckdb/common/common.hpp"

namespace duckdb {

class Value;

Value ConvertVectorToValue(vector<Value> set);
vector<bool> ParseColumnList(const vector<Value> &set, vector<string> &names, const string &option_name);
vector<bool> ParseColumnList(const Value &value, vector<string> &names, const string &option_name);
vector<idx_t> ParseColumnsOrdered(const vector<Value> &set, vector<string> &names, const string &loption);
vector<idx_t> ParseColumnsOrdered(const Value &value, vector<string> &names, const string &loption);

} // namespace duckdb
