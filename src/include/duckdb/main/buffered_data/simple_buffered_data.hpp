//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/simple_buffered_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/buffered_data/buffered_data.hpp"
#include "duckdb/common/thread_annotation.hpp"
#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/common/queue.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {

class StreamQueryResult;

class SimpleBufferedData : public BufferedData {
public:
	static constexpr const BufferedData::Type TYPE = BufferedData::Type::SIMPLE;

public:
	SimpleBufferedData(ClientContext &context, ResultLifetime lifetime);
	~SimpleBufferedData() override;

public:
	//! Buffer a copy of the chunk, or block the sink when the chunk does not fit.
	bool AppendOrBlock(DataChunk &chunk, const InterruptState &blocked_sink);
	//! Whether the buffer is saturated, t.e. can't accept a new chunk right now.
	bool BufferSaturated();
	//! The highest number of bytes the buffer ever held.
	idx_t PeakBufferedBytes() override;
	bool HasBlockedSink() override;
	void UnblockSinks() override;
	void AssertNoBlockedSinks() override;
	unique_ptr<DataChunk> Scan() override;
	inline idx_t BufferSize() const {
		return buffer_size;
	}

protected:
	//! The buffer will not accept more input, and blocking requires a non-empty buffer,
	//! so a saturated buffer always holds a poppable chunk
	bool ReplenishSatisfied() override {
		return BufferSaturated();
	}

private:
	//! Pop restartable sinks into to_unblock.
	void CollectRestartableSinks(annotated_lock_guard<annotated_mutex> &lock, vector<BlockedSink> &to_unblock)
	    DUCKDB_REQUIRES(glock);
	//! Invoke the collected restarts.
	void InvokeUnblocks(const vector<BlockedSink> &to_unblock) DUCKDB_EXCLUDES(glock);

private:
	//! Our handles to reschedule the blocked sink tasks
	queue<BlockedSink> blocked_sinks DUCKDB_GUARDED_BY(glock);
	//! The unread chunks, in arrival order. The buffer owns each copy until its pop
	queue<BufferedChunk> unread_chunks DUCKDB_GUARDED_BY(glock);
	//! The bytes currently buffered
	atomic<idx_t> buffered_count;
	//! The byte cap of the buffer
	const idx_t buffer_size;
	//! The highest number of bytes ever buffered
	idx_t peak_buffered_bytes DUCKDB_GUARDED_BY(glock) = 0;
};

} // namespace duckdb
