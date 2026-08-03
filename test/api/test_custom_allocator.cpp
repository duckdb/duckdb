#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/storage/block_allocator.hpp"
#include "duckdb/storage/storage_info.hpp"

using namespace duckdb;

struct MyAllocateData : public PrivateAllocatorData {
	MyAllocateData(atomic<idx_t> *memory_counter_p) : memory_counter(memory_counter_p) {
	}

	atomic<idx_t> *memory_counter;
};

data_ptr_t my_allocate_function(PrivateAllocatorData *private_data, idx_t size) {
	auto my_allocate_data = (MyAllocateData *)private_data;
	*my_allocate_data->memory_counter += size;
	return data_ptr_cast(malloc(size));
}

void my_free_function(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t size) {
	auto my_allocate_data = (MyAllocateData *)private_data;
	*my_allocate_data->memory_counter -= size;
	free(pointer);
}

data_ptr_t my_reallocate_function(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t old_size, idx_t size) {
	auto my_allocate_data = (MyAllocateData *)private_data;
	*my_allocate_data->memory_counter -= old_size;
	*my_allocate_data->memory_counter += size;
	return data_ptr_cast(realloc(pointer, size));
}

TEST_CASE("Test using a custom allocator", "[api][.]") {
	atomic<idx_t> memory_counter;
	memory_counter = 0;

	REQUIRE(memory_counter.load() == 0);

	DBConfig config;
	config.SetAllocator(make_uniq<Allocator>(my_allocate_function, my_free_function, my_reallocate_function,
	                                         make_uniq<MyAllocateData>(&memory_counter)));
	DuckDB db(nullptr, &config);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl AS SELECT * FROM range(1000000)"));

	// check that the memory counter reported anything
	REQUIRE(memory_counter.load() > 0);
	auto table_memory_usage = memory_counter.load();

	REQUIRE_NO_FAIL(con.Query("DROP TABLE tbl"));

	// check that the memory counter usage has decreased after we dropped the table
	REQUIRE(memory_counter.load() < table_memory_usage);
}

TEST_CASE("Custom allocator and shared memory manager are mutually exclusive", "[api][.]") {
	DuckDB source;
	atomic<idx_t> memory_counter;
	memory_counter = 0;
	auto make_allocator = [&]() {
		return make_uniq<Allocator>(my_allocate_function, my_free_function, my_reallocate_function,
		                            make_uniq<MyAllocateData>(&memory_counter));
	};

	SECTION("SetAllocator after ShareMemoryWith") {
		DBConfig config;
		config.ShareMemoryWith(*source.instance);
		REQUIRE_THROWS_AS(config.SetAllocator(make_allocator()), InvalidInputException);
	}

	SECTION("ShareMemoryWith after SetAllocator") {
		DBConfig config;
		config.SetAllocator(make_allocator());
		REQUIRE_THROWS_AS(config.ShareMemoryWith(*source.instance), InvalidInputException);
	}
}

TEST_CASE("Custom block allocator and shared memory manager are mutually exclusive", "[api][.]") {
	DuckDB source;

	SECTION("SetBlockAllocator after ShareMemoryWith") {
		Allocator allocator;
		DBConfig config;
		config.ShareMemoryWith(*source.instance);
		auto block_allocator =
		    make_uniq<BlockAllocator>(allocator, DEFAULT_BLOCK_ALLOC_SIZE, DEFAULT_BLOCK_ALLOC_SIZE * 16, 0);
		REQUIRE_THROWS_AS(config.SetBlockAllocator(std::move(block_allocator)), InvalidInputException);
	}

	SECTION("ShareMemoryWith after SetBlockAllocator") {
		Allocator allocator;
		DBConfig config;
		auto block_allocator =
		    make_uniq<BlockAllocator>(allocator, DEFAULT_BLOCK_ALLOC_SIZE, DEFAULT_BLOCK_ALLOC_SIZE * 16, 0);
		config.SetBlockAllocator(std::move(block_allocator));
		REQUIRE_THROWS_AS(config.ShareMemoryWith(*source.instance), InvalidInputException);
	}
}
