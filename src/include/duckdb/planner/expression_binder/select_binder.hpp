//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/select_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression_binder/base_select_binder.hpp"

namespace duckdb {

//! The SELECT binder is responsible for binding an expression within the SELECT clause of a SQL statement
class SelectBinder : public BaseSelectBinder {
public:
	SelectBinder(Binder &binder, ClientContext &context, BoundSelectNode &node, BoundGroupInformation &info);

protected:
	BindResult BindUnnest(FunctionExpression &function, idx_t depth, bool root_expression) override;

	idx_t unnest_level = 0;
};

} // namespace duckdb
