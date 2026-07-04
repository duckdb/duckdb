//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/prefetched_file_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

//! A prefetched header prefix, read once while detecting the file type (magic bytes) and reused when the
//! database is opened so the header reads are served from memory instead of issuing more reads. This mostly
//! benefits remote files, where each read is a separate network request.
//! Only the bytes are carried, not an open handle: the actual database handle is opened later (see
//! DatabaseHandle::Open), so we never hold a handle to a file that may turn out to redirect elsewhere.
//! The bytes are shared (read-only after capture) so the struct can ride along copyable option structs.
struct PrefetchedFileData {
	//! Bytes prefetched from offset 0 (covers the MainHeader and both DatabaseHeaders). May be shorter than
	//! requested for a small file.
	shared_ptr<const string> header;

	//! Whether a usable prefetched header prefix is present.
	bool HasHeader() const {
		return header && !header->empty();
	}
};

} // namespace duckdb
