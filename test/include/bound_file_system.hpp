//===----------------------------------------------------------------------===//
//                         DuckDB
//
// bound_file_system.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/file_system.hpp"

namespace duckdb {

//! Binds a BaseFileSystem to an (optional) FileOpener, for tests that exercise custom file systems
class BoundFileSystem : public FileSystem {
public:
	BoundFileSystem(BaseFileSystem &file_system_p, optional_ptr<FileOpener> opener_p)
	    : file_system(file_system_p), opener(opener_p) {
	}

	BaseFileSystem &GetFileSystem() const override {
		return file_system;
	}
	optional_ptr<FileOpener> GetOpener() const override {
		return opener;
	}

private:
	BaseFileSystem &file_system;
	optional_ptr<FileOpener> opener;
};

} // namespace duckdb
