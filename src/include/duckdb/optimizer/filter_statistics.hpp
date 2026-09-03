//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/filter_statistics.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

class LogicalOperator;
class Optimizer;

class FilterStatisticsOptimizer {
public:
	explicit FilterStatisticsOptimizer(Optimizer &optimizer);

	void Optimize(unique_ptr<LogicalOperator> &plan);

private:
	bool HasMultiBindingFilter(const LogicalOperator &op) const;
	bool ContainsDelimJoin(const LogicalOperator &op) const;

private:
	Optimizer &optimizer;
};

} // namespace duckdb
