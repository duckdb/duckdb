//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/expression_binder/default_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/expression_binder.hpp"

namespace duckdb {
struct CreateTableInformation;

//! The Default binder is responsible for binding the DEFAULT clause of a column in a CREATE TABLE statement
class DefaultBinder : public ExpressionBinder {
public:
	DefaultBinder(Binder &binder, ClientContext &context, CreateTableInformation &info);

	CreateTableInformation &info;
protected:
	BindResult BindExpression(ParsedExpression &expr, uint32_t depth, bool root_expression = false) override;
};

} // namespace duckdb
