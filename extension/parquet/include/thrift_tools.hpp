#pragma once
#include <list>
#include "thrift/protocol/TCompactProtocol.h"
#include "thrift/transport/TBufferTransports.h"

#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/allocator.hpp"
#endif

namespace duckdb {

// A ReadHead for prefetching data in a specific range
struct ReadHead {
	ReadHead(idx_t location, uint64_t size) : location(location), size(size) {};
	// Hint info
	idx_t location;
	uint64_t size;

	// Current info
	unique_ptr<AllocatedData> data;
	bool data_isset = false;

	idx_t GetEnd() const {
		return size + location;
	}

	void Allocate(Allocator &allocator) {
		data = allocator.Allocate(size);
	}
};

// Comparator for ReadHeads that are either overlapping, adjacent, or within ALLOW_GAP bytes from each other
struct ReadHeadComparator {
	static constexpr uint64_t ALLOW_GAP = 1 << 14; // 16 KiB
	bool operator()(const ReadHead *a, const ReadHead *b) const {
		auto a_start = a->location;
		auto a_end = a->location + a->size;
		auto b_start = b->location;

		if (a_end <= NumericLimits<idx_t>::Maximum() - ALLOW_GAP) {
			a_end += ALLOW_GAP;
		}

		return a_start < b_start && a_end < b_start;
	}
};

// Two-step read ahead buffer
// 1: register all ranges that will be read, merging ranges that are consecutive
// 2: prefetch all registered ranges
struct ReadAheadBuffer {
	ReadAheadBuffer(Allocator &allocator, FileHandle &handle, FileOpener &opener)
	    : allocator(allocator), handle(handle), file_opener(opener) {
	}

	// The list of read heads
	std::list<ReadHead> read_heads;
	// Set for merging consecutive ranges
	std::set<ReadHead *, ReadHeadComparator> merge_set;

	Allocator &allocator;
	FileHandle &handle;
	FileOpener &file_opener;

	idx_t total_size = 0;

	// Add a read head to the prefetching list
	void AddReadHead(idx_t pos, uint64_t len, bool merge_buffers = true) {
		// Attempt to merge with existing
		if (merge_buffers) {
			ReadHead new_read_head {pos, len};
			auto lookup_set = merge_set.find(&new_read_head);
			if (lookup_set != merge_set.end()) {
				auto existing_head = *lookup_set;
				auto new_start = MinValue<idx_t>(existing_head->location, new_read_head.location);
				auto new_length = MaxValue<idx_t>(existing_head->GetEnd(), new_read_head.GetEnd()) - new_start;
				existing_head->location = new_start;
				existing_head->size = new_length;
				return;
			}
		}

		read_heads.emplace_front(ReadHead(pos, len));
		total_size += len;
		auto &read_head = read_heads.front();

		if (merge_buffers) {
			merge_set.insert(&read_head);
		}

		if (read_head.GetEnd() > handle.GetFileSize()) {
			throw std::runtime_error("Prefetch registered for bytes outside file");
		}
	}

	// Returns the relevant read head
	ReadHead *GetReadHead(idx_t pos) {
		for (auto &read_head : read_heads) {
			if (pos >= read_head.location && pos < read_head.GetEnd()) {
				return &read_head;
			}
		}
		return nullptr;
	}

	// Prefetch all read heads
	void Prefetch() {
		for (auto &read_head : read_heads) {
			read_head.Allocate(allocator);

			if (read_head.GetEnd() > handle.GetFileSize()) {
				throw std::runtime_error("Prefetch registered requested for bytes outside file");
			}

			handle.Read(read_head.data->get(), read_head.size, read_head.location);
			read_head.data_isset = true;
		}
	}
};

class ThriftFileTransport : public duckdb_apache::thrift::transport::TVirtualTransport<ThriftFileTransport> {
public:
	static constexpr uint64_t PREFETCH_FALLBACK_BUFFERSIZE = 1000000;

	ThriftFileTransport(Allocator &allocator, FileHandle &handle_p, FileOpener &opener, bool prefetch_mode_p)
	    : handle(handle_p), location(0), allocator(allocator), ra_buffer(ReadAheadBuffer(allocator, handle_p, opener)),
	      prefetch_mode(prefetch_mode_p) {
	}

	uint32_t read(uint8_t *buf, uint32_t len) {
		auto prefetch_buffer = ra_buffer.GetReadHead(location);
		if (prefetch_buffer != nullptr && location - prefetch_buffer->location + len <= prefetch_buffer->size) {
			D_ASSERT(location - prefetch_buffer->location + len <= prefetch_buffer->size);

			if (!prefetch_buffer->data_isset) {
				prefetch_buffer->Allocate(allocator);
				handle.Read(prefetch_buffer->data->get(), prefetch_buffer->size, prefetch_buffer->location);
				prefetch_buffer->data_isset = true;
			}
			memcpy(buf, prefetch_buffer->data->get() + location - prefetch_buffer->location, len);
		} else {
			if (prefetch_mode && len < PREFETCH_FALLBACK_BUFFERSIZE && len > 0) {
				Prefetch(location, MinValue<uint64_t>(PREFETCH_FALLBACK_BUFFERSIZE, handle.GetFileSize() - location));
				auto prefetch_buffer_fallback = ra_buffer.GetReadHead(location);
				D_ASSERT(location - prefetch_buffer_fallback->location + len <= prefetch_buffer_fallback->size);
				memcpy(buf, prefetch_buffer_fallback->data->get() + location - prefetch_buffer_fallback->location, len);
			} else {
				handle.Read(buf, len, location);
			}
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

	// Register a buffer for prefixing
	void RegisterPrefetch(idx_t pos, uint64_t len, bool can_merge = true) {
		ra_buffer.AddReadHead(pos, len, can_merge);
	}

	// Prevents any further merges, should be called before PrefetchRegistered
	void FinalizeRegistration() {
		ra_buffer.merge_set.clear();
	}

	// Prefetch all previously registered ranges
	void PrefetchRegistered() {
		ra_buffer.Prefetch();
	}

	void ClearPrefetch() {
		ra_buffer.read_heads.clear();
		ra_buffer.merge_set.clear();
	}

	void SetLocation(idx_t location_p) {
		location = location_p;
	}

	idx_t GetLocation() {
		return location;
	}
	idx_t GetSize() {
		return handle.file_system.GetFileSize(handle);
	}

private:
	FileHandle &handle;
	idx_t location;

	Allocator &allocator;

	// Multi-buffer prefetch
	ReadAheadBuffer ra_buffer;

	// Whether the prefetch mode is enabled. In this mode the DirectIO flag of the handle will be set and the parquet
	// reader will manage the read buffering.
	bool prefetch_mode;
};

} // namespace duckdb
