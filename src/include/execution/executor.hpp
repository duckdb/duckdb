//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// execution/executor.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"

#include "duckdb.hpp"

namespace duckdb {

//! Executor is responsible for executing a physical operator plan and
//! outputting a result object
class Executor {
  public:
	//! Execute the specified physical operator plan
	std::unique_ptr<DuckDBResult> Execute(std::unique_ptr<PhysicalOperator> op);
};
} // namespace duckdb
