//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/pipeline_execution_order.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class PipelineExecutionOrder : uint8_t { PARTITIONED, ORDERED, UNORDERED };

} // namespace duckdb
