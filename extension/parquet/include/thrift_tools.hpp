#pragma once

#include "thrift/protocol/TCompactProtocol.h"
#include "thrift/transport/TBufferTransports.h"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

class ThriftFileTransport : public apache::thrift::transport::TVirtualTransport<ThriftFileTransport> {
public:
	ThriftFileTransport(unique_ptr<FileHandle> handle_p) : handle(move(handle_p)), location(0) {
	}

	uint32_t read(uint8_t *buf, uint32_t len) {
		handle->Read(buf, len, location);
		location += len;
		return len;
	}

	void SetLocation(idx_t location_p) {
		location = location_p;
	}

	idx_t GetLocation() {
		return location;
	}

private:
	unique_ptr<duckdb::FileHandle> handle;
	duckdb::idx_t location;
};

} // namespace duckdb
