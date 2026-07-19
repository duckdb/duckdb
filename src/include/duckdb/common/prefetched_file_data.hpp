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

//! Header bytes read once during file-type detection and reused when opening the database, so the header
//! reads hit memory instead of re-reading (mainly helps remote files). Bytes only, no open handle.
struct PrefetchedFileData {
	//! Bytes prefetched from offset 0 (MainHeader + both DatabaseHeaders); may be shorter for a small file.
	shared_ptr<const string> header;

	//! Whether a usable prefetched header prefix is present.
	bool HasHeader() const {
		return header && !header->empty();
	}
};

} // namespace duckdb
