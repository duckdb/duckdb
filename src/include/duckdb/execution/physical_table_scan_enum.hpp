//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/physical_table_scan_enum.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <stdint.h>

namespace duckdb {

enum class PhysicalTableScanExecutionStrategy : uint8_t {
	DEFAULT,
	TASK_EXECUTOR,
	SYNCHRONOUS,
	TASK_EXECUTOR_BUT_FORCE_SYNC_CHECKS
};

}; // namespace duckdb
