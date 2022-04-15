#pragma once
#include <iostream>
#include "thrift/protocol/TCompactProtocol.h"
#include "thrift/transport/TBufferTransports.h"

#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/allocator.hpp"
#endif

namespace duckdb {

struct PrefetchBuffer {
	PrefetchBuffer(idx_t location, size_t size, Allocator &allocator) : location(location) {
		data = allocator.Allocate(size);
	}
	idx_t location;
	unique_ptr<AllocatedData> data;
	bool operator == (const PrefetchBuffer& other) const { return data == other.data; }
};

/// A ReadHead contains a range that was hinted to be accessed in the near future. The read head will hold cached data
/// within that range.
struct ReadHead {
	ReadHead(idx_t location, size_t size) : location(location), size(size), current_end(location){};
	/// Hint info
	idx_t location;
	size_t size;

	/// Current info
	idx_t current_location;
	unique_ptr<AllocatedData> data;
	idx_t current_end;

	idx_t GetEnd() const {
		return size + location;
	}

	size_t RemainingBytes() {
		return GetEnd() - current_end;
	}

	void Allocate(Allocator &allocator, size_t allocate_size) {
		data = allocator.Allocate(allocate_size);
		current_location = current_end;
		current_end = current_location + allocate_size;
	}

	bool operator == (const PrefetchBuffer& other) const { return location == other.location; }
};

/// Read ahead buffer implementation that requires specifying the ranges that can be accesssed
struct ReadAheadBuffer {
	ReadAheadBuffer(size_t total_size) : total_size(total_size), currently_allocated(0){};

	/// These ReadHeads currently hold data that we expect to read soon
	std::list<ReadHead> in_use;
	/// These ReadHeads have currently no buffer attached but are available for more prefetching
	std::list<ReadHead> stand_by;

	idx_t total_size;
	idx_t currently_allocated;

	/// Add a read head to the prefetching list
	void AddReadHead(idx_t pos, idx_t len) {
		stand_by.emplace_front(ReadHead(pos, len));
	}

	/// Returns the relevant read head
	ReadHead* GetReadHead(idx_t pos) {
		for (auto& read_head: in_use) {
			if (pos >= read_head.location && pos < read_head.GetEnd()) {
				return &read_head;
			}
		}
		return nullptr;
	}

	// Prefetch all readheads filling up until our allocation limit
	void PrefetchAll(Allocator &allocator, FileHandle &handle) {
		if (!stand_by.empty()) {
			// first see how much space we have
			auto available =  total_size - currently_allocated;

			// Initially we attempt to equally divide available space over readheads
			vector<size_t> allocation_sizes;
			size_t allocated = 0;
			allocation_sizes.reserve(stand_by.size());
			size_t initial_space_per_head = available / stand_by.size();
			idx_t idx = 0;
			for (auto& head: stand_by) {
				auto to_allocate = MinValue(initial_space_per_head, head.RemainingBytes());
				allocated += to_allocate;
				allocation_sizes[idx++] = to_allocate;
			}

			// Assign the ramaining bytes to whoever wants them TODO: can we do better?
			// Then also allocate the ReadHead
			idx = 0;
			for (auto& head: stand_by) {
				if (allocated == available) {
					break;
				}
				auto to_allocate = MaxValue<size_t>(0, head.RemainingBytes() - allocation_sizes[idx]);
				allocated += to_allocate;
				allocation_sizes[idx] += to_allocate;

				head.Allocate(allocator, allocation_sizes[idx]);
				handle.Read(head.data->get(), allocation_sizes[idx], head.current_location);

				idx++;
			}
		}
	}

	/// Read the through the RA buffer.
	uint32_t Read(idx_t pos, size_t len, FileHandle& handle) {

	}
};

class ThriftFileTransport : public duckdb_apache::thrift::transport::TVirtualTransport<ThriftFileTransport> {
public:
	ThriftFileTransport(Allocator &allocator, FileHandle &handle_p)
	    : allocator(allocator), handle(handle_p), location(0) {
	}

	uint32_t read(uint8_t *buf, uint32_t len) {
		if (prefetched_data && location >= prefetch_location &&
		    location + len < prefetch_location + prefetched_data->GetSize()) {
			memcpy(buf, prefetched_data->get() + location - prefetch_location, len);
		} else {
			auto prefetch_buffer = GetPrefetchBuffer(location, len);
			if (prefetch_buffer != nullptr) {
				memcpy(buf, prefetch_buffer->data->get() + location - prefetch_buffer->location, len);
			} else {
				handle.Read(buf, len, location);
			}
		}
		location += len;
		return len;
	}

	// TODO: make this a buffering hint to allow limiting memory
	void PrefetchBuf(idx_t pos, idx_t len) {
		std::cout << "Prefetching Buf: " << pos << " till " << pos + len << " bytes \n";
		prefetch_buffers.emplace_front(PrefetchBuffer(pos, len, allocator));
		handle.Read(prefetch_buffers.front().data->get(), len, pos);
	}

	void ClearPrefetchBuffer(idx_t pos) {
		prefetch_buffers.remove(*GetPrefetchBuffer(pos, 0));
	}

	void ClearPrefetchBuffers(idx_t pos) {
	    prefetch_buffers.clear();
	}

	void Prefetch(idx_t pos, idx_t len) {
		std::cout << "Prefetching: " << pos << " till " << pos + len << " bytes \n";
		prefetch_location = pos;
		prefetched_data = allocator.Allocate(len);
		handle.Read(prefetched_data->get(), len, prefetch_location); // here
	}

	void ClearPrefetch() {
		prefetched_data.reset();
		prefetched_data = nullptr;
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

protected:
	PrefetchBuffer* GetPrefetchBuffer(idx_t pos, size_t len) {
		for (auto& prefetch_buffer: prefetch_buffers) {
			if (pos >= prefetch_buffer.location &&
			    pos + len < prefetch_location + prefetch_buffer.data->GetSize()) {
				return &prefetch_buffer;
			}
		}
		return nullptr;
	}

private:
	Allocator &allocator;
	FileHandle &handle;
	idx_t location;

	// Main full row_group prefetch
	unique_ptr<AllocatedData> prefetched_data;
	idx_t prefetch_location;

	// Multi-buffer prefetch
	std::list<PrefetchBuffer> prefetch_buffers;
};

} // namespace duckdb
