//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/bound_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/enums/set_operation_type.hpp"
#include "duckdb/common/shared_ptr.hpp"

namespace duckdb {

class LogicalOperator;
struct LogicalType;
struct BoundStatement;
class ParsedExpression;
class Binder;

//! A parsed projection and its final position and CASE compatibility after binding.
struct SelectExpressionInfo {
	unique_ptr<ParsedExpression> expression;
	idx_t index;
	bool case_compatible;
};

struct ExtraBoundInfo {
	SetOperationType setop_type = SetOperationType::NONE;
	vector<shared_ptr<Binder>> child_binders;
	vector<BoundStatement> bound_children;
	vector<SelectExpressionInfo> original_expressions;
};

struct BoundStatement {
	unique_ptr<LogicalOperator> plan;
	vector<LogicalType> types;
	vector<Identifier> names;
	ExtraBoundInfo extra_info;
};

} // namespace duckdb
