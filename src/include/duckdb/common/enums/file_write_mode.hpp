//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/file_write_mode.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

//! The write ordering contract exposed by a file handle.
enum class FileWriteMode : uint8_t {
	//! Writes use the file handle's stream position and must execute one at a time.
	SEQUENTIAL,
	//! Contiguous writes receive logical offsets; the target admits them in order before backend work may overlap.
	CONCURRENT_SEQUENTIAL,
	//! Independent ranges may be written at arbitrary offsets and in arbitrary order.
	POSITIONAL
};

} // namespace duckdb
