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
struct BoundOrderByNode;
struct BoundStatement;
class Binder;

Value ConvertVectorToValue(vector<Value> set);
vector<bool> ParseColumnList(const vector<Value> &set, vector<string> &names, const string &option_name);
vector<bool> ParseColumnList(const Value &value, vector<string> &names, const string &option_name);
vector<idx_t> ParseColumnsOrdered(const vector<Value> &set, const vector<string> &names, const string &loption);
vector<idx_t> ParseColumnsOrdered(const Value &value, const vector<string> &names, const string &loption);
vector<BoundOrderByNode> ParseOrderByColumns(Binder &binder, const vector<Value> &set,
                                             const BoundStatement &bound_statement, const string &loption);

} // namespace duckdb
