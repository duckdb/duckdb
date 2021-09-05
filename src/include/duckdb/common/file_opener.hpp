//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/file_opener.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

//! Abstact type that provide client-spcific context to FileSystem.
class FileOpener {
public:
	virtual ~FileOpener() {};
};

} // namespace duckdb