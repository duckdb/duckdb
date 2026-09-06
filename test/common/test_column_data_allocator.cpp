#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/common/types/column/column_data_allocator.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "test_helpers.hpp"

namespace duckdb {

TEST_CASE("ColumnDataAllocator copy constructor preserves managed_result_set", "[column_data_allocator]") {
	DuckDB db(":memory:");
	auto &buffer_manager = BufferManager::GetBufferManager(*db.instance);

	ColumnDataAllocator original(buffer_manager, ColumnDataCollectionLifetime::THROW_ERROR_AFTER_DATABASE_CLOSES);
	REQUIRE(original.GetDatabase() != nullptr);

	ColumnDataAllocator copied(original);
	REQUIRE(copied.GetDatabase() != nullptr);
	REQUIRE(copied.GetType() == ColumnDataAllocatorType::BUFFER_MANAGER_ALLOCATOR);
}

} // namespace duckdb
