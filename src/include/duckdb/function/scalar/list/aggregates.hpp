//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar/aggregates.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function_set.hpp"

namespace duckdb {

struct ListAggregatesBindData : public FunctionData {
	ListAggregatesBindData(const LogicalType &stype_p, AggregateFunction aggr_functio_p);
	~ListAggregatesBindData() override;

	LogicalType stype;
	AggregateFunction aggr_function;

	unique_ptr<FunctionData> Copy() override;
};

} // namespace duckdb
