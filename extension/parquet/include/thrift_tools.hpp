#pragma once

#include "thrift/protocol/TCompactProtocol.h"
#include "thrift/transport/TBufferTransports.h"

#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/file_system.hpp"
#endif

namespace duckdb {

class ThriftFileTransport : public duckdb_apache::thrift::transport::TVirtualTransport<ThriftFileTransport> {
public:
	ThriftFileTransport(FileHandle &handle_p) : handle(handle_p), location(0) {
	}

	uint32_t read(uint8_t *buf, uint32_t len) {
		handle.Read(buf, len, location);
		location += len;
		return len;
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
	duckdb::FileHandle &handle;
	duckdb::idx_t location;
};

} // namespace duckdb
