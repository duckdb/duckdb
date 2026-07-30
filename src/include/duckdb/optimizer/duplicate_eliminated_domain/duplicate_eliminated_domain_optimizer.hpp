//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {

class LogicalOperator;
class Optimizer;

//! Optimizes and lowers duplicate-eliminated joins after the first filter-pushdown pass.
class DuplicateEliminatedDomainOptimizer {
public:
	explicit DuplicateEliminatedDomainOptimizer(Optimizer &optimizer);

	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> plan);

private:
	Optimizer &optimizer;
};

} // namespace duckdb
