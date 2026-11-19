//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/extension/capi_allocator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/allocator.hpp"

namespace duckdb {

struct ExtensionAllocator {
	static unique_ptr<Allocator> Create();
	static unique_ptr<Allocator> Create(DatabaseInstance &db);
	static void Register(Allocator &allocator, const string &extension_name);
	static optional_idx GetUsage(const string &extension_name);
};

} // namespace duckdb
