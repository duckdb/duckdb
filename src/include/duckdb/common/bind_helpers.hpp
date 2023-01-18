//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/bind_helpers.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

namespace duckdb {

Value ConvertVectorToValue(vector<Value> set);
vector<bool> ParseColumnList(const vector<Value> &set, vector<string> &names, const string &option_name);
vector<bool> ParseColumnList(const Value &value, vector<string> &names, const string &option_name);

} // namespace duckdb
