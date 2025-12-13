//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/count_window_elimination.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/optimizer.hpp"

#include "duckdb/optimizer/column_binding_replacer.hpp"

namespace duckdb {

class CountWindowElimination {
public:
	explicit CountWindowElimination(Optimizer &optimizer);

	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

private:
	unique_ptr<LogicalOperator> OptimizeInternal(unique_ptr<LogicalOperator> op, ColumnBindingReplacer &replacer);

	Optimizer &optimizer;
};

} // namespace duckdb
