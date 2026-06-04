//===----------------------------------------------------------------------===//
//                         DuckDB
//
// thrift_tools.hpp
//
//
//===----------------------------------------------------------------------===/

#pragma once

#include <list>
#include "thrift/protocol/TCompactProtocol.h"
#include "thrift/transport/TBufferTransports.h"

#include "duckdb.hpp"
#include "duckdb/storage/external_file_cache/caching_file_system.hpp"
#include "duckdb/storage/external_file_cache/file_buffer_handle_group.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/allocator.hpp"

namespace duckdb {

// A ReadHead for prefetching data in a specific range
struct ReadHead {
	ReadHead(idx_t location, uint64_t size) : location(location), size(size) {};
	// Hint info
	idx_t location;
	uint64_t size;

	// Current info
	FileBufferHandleGroup handle_group;
	AllocatedData local_buffer;
	const_data_ptr_t buffer_ptr = nullptr;
	bool data_isset = false;

	idx_t GetEnd() const {
		return size + location;
	}

	// Materialize [buffer_ptr], should call before access.
	// TODO(hjiang): Currently it's only used for `Prefetch` operation, should be able to save one copy.
	void Materialize(Allocator &allocator) {
		if (handle_group.GetHandles().size() == 1) {
			buffer_ptr = handle_group.Ptr();
		} else {
			local_buffer = allocator.Allocate(size);
			handle_group.CopyTo(local_buffer.get(), size);
			buffer_ptr = local_buffer.get();
		}
	}
};

// Maximum gap between prefetch ranges that can still be coalesced.
static constexpr uint64_t DEFAULT_ACCEPTED_COLUMN_GAP = 1 << 14; // 16 KiB

// Read ahead buffer
// 1: register all ranges that will be read
// 2: keep mergeable ranges pending during registration
// 3: finalize by sorting and coalescing pending ranges
// 4: prefetch finalized ranges
struct ReadAheadBuffer {
	explicit ReadAheadBuffer(CachingFileHandle &file_handle_p,
	                         uint64_t accepted_column_gap = DEFAULT_ACCEPTED_COLUMN_GAP)
	    : file_handle(file_handle_p), accepted_column_gap(accepted_column_gap) {
	}

	// Finalized read heads ready for lookup and prefetch.
	std::list<ReadHead> read_heads;

	// Pending mergeable read heads collected during registration.
	std::list<ReadHead> merge_read_heads;

	CachingFileHandle &file_handle;

	uint64_t accepted_column_gap;

	void SetAcceptedColumnGap(uint64_t accepted_column_gap_p) {
		D_ASSERT(merge_read_heads.empty());
		accepted_column_gap = accepted_column_gap_p;
	}

	uint64_t GetAcceptedColumnGap() const {
		return accepted_column_gap;
	}

	// Add a read head to the prefetching list
	void AddReadHead(idx_t pos, uint64_t len, bool can_merge = true) {
		ValidateRange(pos, len);

		if (can_merge) {
			merge_read_heads.emplace_front(pos, len);
		} else {
			read_heads.emplace_front(pos, len);
		}
	}

	// Returns the relevant read head
	ReadHead *GetReadHead(idx_t pos) {
		FinalizeRegistration();

		for (auto &read_head : read_heads) {
			if (pos >= read_head.location && pos < read_head.GetEnd()) {
				return &read_head;
			}
		}
		return nullptr;
	}

	// Prefetch all read heads
	void Prefetch() {
		FinalizeRegistration();

		for (auto &read_head : read_heads) {
			if (read_head.data_isset) {
				continue;
			}
			read_head.handle_group = file_handle.Read(read_head.size, read_head.location);
			read_head.Materialize(file_handle.GetBufferAllocator());
			read_head.data_isset = true;
		}
	}

	// Merge pending read heads and move them to the finalized list.
	void FinalizeRegistration() {
		if (merge_read_heads.empty()) {
			return;
		}

		// Sort by start offset so the following adjacent scan can coalesce cascading ranges.
		merge_read_heads.sort(
		    [](const ReadHead &left, const ReadHead &right) { return left.location < right.location; });

		// Walk adjacent ranges and keep extending the current range while the next one can be merged into it.
		auto current = merge_read_heads.begin();
		auto next = current;
		++next;
		while (next != merge_read_heads.end()) {
			D_ASSERT(current->location <= next->location);

			if (current->GetEnd() >= next->location || next->location - current->GetEnd() <= accepted_column_gap) {
				// Merge overlapping, adjacent, or nearby ranges into current.
				auto new_end = MaxValue<idx_t>(current->GetEnd(), next->GetEnd());
				current->size = new_end - current->location;
				next = merge_read_heads.erase(next);
			} else {
				current = next;
				++next;
			}
		}

		read_heads.splice(read_heads.begin(), merge_read_heads);
	}

	// Drop both finalized and pending read heads.
	void Clear() {
		read_heads.clear();
		merge_read_heads.clear();
	}

	// Pending read heads also count as prefetch state.
	bool HasPrefetch() const {
		return !read_heads.empty() || !merge_read_heads.empty();
	}

private:
	// Reject ranges outside the file before they enter either read-head list.
	void ValidateRange(idx_t pos, uint64_t len) {
		auto file_size = file_handle.GetFileSize();

		if (pos > file_size || len > file_size - pos) {
			auto end = len > NumericLimits<idx_t>::Maximum() - pos ? string("overflow") : std::to_string(pos + len);

			throw std::runtime_error("Prefetch registered for bytes outside file: " + file_handle.GetPath() +
			                         ", attempted range: [" + std::to_string(pos) + ", " + end +
			                         "), file size: " + std::to_string(file_size));
		}
	}
};

class ThriftFileTransport : public duckdb_apache::thrift::transport::TVirtualTransport<ThriftFileTransport> {
public:
	static constexpr uint64_t PREFETCH_FALLBACK_BUFFERSIZE = 1000000;

	ThriftFileTransport(CachingFileHandle &file_handle_p, bool prefetch_mode_p,
	                    uint64_t accepted_column_gap = DEFAULT_ACCEPTED_COLUMN_GAP)
	    : file_handle(file_handle_p), location(0), size(file_handle.GetFileSize()),
	      ra_buffer(ReadAheadBuffer(file_handle, accepted_column_gap)), prefetch_mode(prefetch_mode_p) {
	}

	void SetAcceptedColumnGap(uint64_t accepted_column_gap) {
		ra_buffer.SetAcceptedColumnGap(accepted_column_gap);
	}

	// The accepted column gap currently used to coalesce adjacent ranges.
	uint64_t GetAcceptedColumnGap() const {
		return ra_buffer.GetAcceptedColumnGap();
	}

	uint32_t read(uint8_t *buf, uint32_t len) {
		auto prefetch_buffer = ra_buffer.GetReadHead(location);
		if (prefetch_buffer != nullptr && location - prefetch_buffer->location + len <= prefetch_buffer->size) {
			D_ASSERT(location - prefetch_buffer->location + len <= prefetch_buffer->size);

			if (!prefetch_buffer->data_isset) {
				prefetch_buffer->handle_group = file_handle.Read(prefetch_buffer->size, prefetch_buffer->location);
				prefetch_buffer->Materialize(file_handle.GetBufferAllocator());
				prefetch_buffer->data_isset = true;
			}
			memcpy(buf, prefetch_buffer->buffer_ptr + location - prefetch_buffer->location, len);
		} else if (prefetch_mode && len < PREFETCH_FALLBACK_BUFFERSIZE && len > 0) {
			Prefetch(location, MinValue<uint64_t>(PREFETCH_FALLBACK_BUFFERSIZE, file_handle.GetFileSize() - location));
			auto prefetch_buffer_fallback = ra_buffer.GetReadHead(location);
			D_ASSERT(location - prefetch_buffer_fallback->location + len <= prefetch_buffer_fallback->size);
			memcpy(buf, prefetch_buffer_fallback->buffer_ptr + location - prefetch_buffer_fallback->location, len);
		} else {
			// No prefetch, do a regular (non-caching) read
			file_handle.GetFileHandle().Read(context, buf, len, location);
		}

		location += len;
		return len;
	}

	// Prefetch a single buffer
	void Prefetch(idx_t pos, uint64_t len) {
		RegisterPrefetch(pos, len, false);
		FinalizeRegistration();
		PrefetchRegistered();
	}

	// Register a buffer for prefetching
	void RegisterPrefetch(idx_t pos, uint64_t len, bool can_merge = true) {
		ra_buffer.AddReadHead(pos, len, can_merge);
	}

	// Finalizes mergeable read heads, should be called before PrefetchRegistered
	void FinalizeRegistration() {
		ra_buffer.FinalizeRegistration();
	}

	// Prefetch all previously registered ranges
	void PrefetchRegistered() {
		ra_buffer.Prefetch();
	}

	void ClearPrefetch() {
		ra_buffer.Clear();
	}

	void Skip(idx_t skip_count) {
		location += skip_count;
	}

	bool HasPrefetch() const {
		return ra_buffer.HasPrefetch();
	}

	void SetLocation(idx_t location_p) {
		location = location_p;
	}

	idx_t GetLocation() const {
		return location;
	}

	optional_ptr<ReadHead> GetReadHead(idx_t pos) {
		return ra_buffer.GetReadHead(pos);
	}

	idx_t GetSize() const {
		return size;
	}

private:
	QueryContext context;

	CachingFileHandle &file_handle;
	idx_t location;
	idx_t size;

	// Multi-buffer prefetch
	ReadAheadBuffer ra_buffer;

	// Whether the prefetch mode is enabled. In this mode the DirectIO flag of the handle will be set and the parquet
	// reader will manage the read buffering.
	bool prefetch_mode;
};

} // namespace duckdb
