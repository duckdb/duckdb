//===----------------------------------------------------------------------===//
//                         DuckDB
//
// zstd_file_system.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/compressed_file_system.hpp"
#endif

namespace duckdb {

class ZStdFileSystem : public CompressedFileSystem {
public:
	unique_ptr<FileHandle> OpenCompressedFile(unique_ptr<FileHandle> handle, bool write) override;

	std::string GetName() const override {
		return "ZStdFileSystem";
	}

	unique_ptr<StreamWrapper> CreateStream() override;
	idx_t InBufferSize() override;
	idx_t OutBufferSize() override;
};

} // namespace duckdb
