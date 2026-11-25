//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table/native_scan_job.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parallel/scan_read_ahead.hpp"
#include "duckdb/storage/table/scan_state.hpp"

namespace duckdb {

//! Unordered read-ahead carries storage ranges, leaving decoding state with its worker.
struct NativeScanJob : public ScanReadAheadJob {
	explicit NativeScanJob(BlockAllocator &allocator) : allocator(allocator) {
	}
	~NativeScanJob() override {
		SettleIO();
	}
	optional_idx ResolveAffinity() override {
		auto hint = blocks.GetCachedShard(allocator);
		blocks.blocks.clear();
		return hint;
	}
	idx_t GetMemoryUsage() const override {
		return sizeof(*this) + blocks.blocks.capacity() * sizeof(shared_ptr<BlockHandle>);
	}

	TableScanAssignment assignment;
	PrefetchState blocks;
	BlockAllocator &allocator;
};

} // namespace duckdb
