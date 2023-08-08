//===----------------------------------------------------------------------===//
//                         DuckDB
//
// jemalloc_extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

class JemallocExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;

	static data_ptr_t Allocate(PrivateAllocatorData *private_data, idx_t size);
	static void Free(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t size);
	static data_ptr_t Reallocate(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t old_size, idx_t size);

	static void ThreadFlush(idx_t threshold);
};

} // namespace duckdb
