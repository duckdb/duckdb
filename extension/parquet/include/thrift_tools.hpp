#pragma once

#include "thrift/protocol/TCompactProtocol.h"
#include "thrift/transport/TBufferTransports.h"

#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/allocator.hpp"
#endif

namespace duckdb {

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
			handle.Read(buf, len, location);
		}
		location += len;
		return len;
	}

	void Prefetch(idx_t pos, idx_t len) {
		prefetch_location = pos;
		prefetched_data = allocator.Allocate(len);
		handle.Read(prefetched_data->get(), len, prefetch_location);
	}

	void ClearPrefetch() {
		prefetched_data.reset();
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
	Allocator &allocator;
	FileHandle &handle;
	idx_t location;

	unique_ptr<AllocatedData> prefetched_data;
	idx_t prefetch_location;
};

} // namespace duckdb
