//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/type_pushdown.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class TypePushdown {
public:
	explicit TypePushdown(ClientContext &ctx);
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

private:
	ClientContext &context;
};

} // namespace duckdb
