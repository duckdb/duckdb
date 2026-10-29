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
class ExpressionMatchDescriptor;

//! A finalized projection match and its position in the output.
struct SelectExpressionInfo {
	shared_ptr<const ExpressionMatchDescriptor> expression;
	idx_t index;
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
