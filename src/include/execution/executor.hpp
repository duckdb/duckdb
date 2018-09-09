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

#include "common/types/chunk_collection.hpp"

#include "execution/physical_operator.hpp"

namespace duckdb {

//! Executor is responsible for executing a physical operator plan and
//! outputting a result object
class Executor {
  public:
	//! Execute the specified physical operator plan
	ChunkCollection Execute(ClientContext &context,
	                        std::unique_ptr<PhysicalOperator> op);
};
} // namespace duckdb
