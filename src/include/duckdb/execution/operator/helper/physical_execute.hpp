//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_execute.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class PhysicalExecute : public PhysicalOperator {
public:
	explicit PhysicalExecute(PhysicalOperator *plan);

	PhysicalOperator *plan;
};

} // namespace duckdb
