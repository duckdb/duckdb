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
	std::string Version() const override;

	static void *malloc(size_t size);
	static void *realloc(void *ptr, size_t size);
	static void free(void *ptr);

	static int64_t DecayDelay();
	static void ThreadFlush(idx_t threshold);
	static void ThreadIdle();
	static void FlushAll();
	static void SetBackgroundThreads(bool enable);
};

} // namespace duckdb
