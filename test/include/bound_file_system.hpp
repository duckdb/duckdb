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

//! Binds a BaseFileSystem to a Context, for tests that exercise custom file systems
class BoundFileSystem : public FileSystem {
public:
	BoundFileSystem(BaseFileSystem &file_system_p, Context &context_p)
	    : file_system(file_system_p), context(context_p) {
	}

protected:
	//! Every path goes to the bound file system, so there is no root to register sub-systems on
	VirtualFileSystem &GetFileSystem() const override {
		throw InternalException("BoundFileSystem has no virtual file system");
	}
	Context &GetContext() const override {
		return context;
	}
	optional_ptr<BaseFileSystem> GetFixedFileSystem() const override {
		return file_system;
	}

private:
	BaseFileSystem &file_system;
	Context &context;
};

} // namespace duckdb
