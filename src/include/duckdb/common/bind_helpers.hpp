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
#include "duckdb/common/identifier.hpp"

namespace duckdb {

class Value;
struct BoundOrderByNode;
struct BoundStatement;
class Binder;

Value ConvertVectorToValue(vector<Value> set);
vector<bool> ParseColumnList(const vector<Value> &set, vector<string> &names, const Identifier &option_name);
vector<bool> ParseColumnList(const Value &value, vector<string> &names, const Identifier &option_name);
vector<idx_t> ParseColumnsOrdered(const vector<Value> &set, const vector<Identifier> &names,
                                  const Identifier &option_name);
vector<idx_t> ParseColumnsOrdered(const Value &value, const vector<Identifier> &names, const Identifier &option_name);
vector<BoundOrderByNode> ParseOrderByColumns(Binder &binder, const vector<Value> &set,
                                             const BoundStatement &bound_statement, const Identifier &option_name);

} // namespace duckdb
