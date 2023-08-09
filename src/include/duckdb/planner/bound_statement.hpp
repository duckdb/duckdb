//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/bound_statement.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/optimizer/cascade/operators/Operator.h"

namespace duckdb
{
using namespace gpopt;

class LogicalOperator;
struct LogicalType;

struct BoundStatement
{
	unique_ptr<Operator> plan;
	vector<LogicalType> types;
	vector<string> names;
};
} // namespace duckdb