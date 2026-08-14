//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/multi_file/multi_file_read_ahead.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parallel/scan_read_ahead.hpp"

namespace duckdb {
struct MultiFileScanJob;
struct LocalTableFunctionState;

//! Drives read-ahead for the multi-file scan, it's purpose is to keep several scan jobs scheduled ahead of decoding
class MultiFileReadAhead : public ScanReadAhead<MultiFileScanJob, LocalTableFunctionState> {
public:
	MultiFileReadAhead(ClientContext &context, idx_t read_ahead_depth,
	                   unique_ptr<ManagedAsyncMemoryGovernor> memory_governor);
	~MultiFileReadAhead();

public:
	//! Create the read-ahead driver from the read_ahead_depth setting.
	//! -1 = automatic: unlimited depth, gated by a temp-memory reservation. Returns null when read-ahead is disabled.
	static unique_ptr<MultiFileReadAhead> Create(ClientContext &context);
};

} // namespace duckdb
