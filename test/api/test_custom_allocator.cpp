#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/common/atomic.hpp"

using namespace duckdb;
using namespace std;

struct MyAllocateData : public PrivateAllocatorData {
	MyAllocateData(atomic<idx_t> *memory_counter_p) : memory_counter(memory_counter_p) {
	}

	atomic<idx_t> *memory_counter;
};

data_ptr_t my_allocate_function(PrivateAllocatorData *private_data, idx_t size) {
	auto my_allocate_data = (MyAllocateData *)private_data;
	*my_allocate_data->memory_counter += size;
	return (data_ptr_t)malloc(size);
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
	return (data_ptr_t)realloc(pointer, size);
}

TEST_CASE("Test using a custom allocator", "[api]") {
	atomic<idx_t> memory_counter;
	memory_counter = 0;

	REQUIRE(memory_counter.load() == 0);

	DBConfig config;
	config.allocator = make_unique<Allocator>(my_allocate_function, my_free_function, my_reallocate_function,
	                                          make_unique<MyAllocateData>(&memory_counter));
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
