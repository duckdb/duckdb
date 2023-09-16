#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test storing a big string that exceeds buffer manager size", "[storage][.]") {
	duckdb::unique_ptr<MaterializedQueryResult> result;
	auto storage_database = TestCreatePath("storage_test");
	auto config = GetTestConfig();
	config->options.maximum_threads = 1;

	uint64_t string_length = 64;
	uint64_t desired_size = 10000000; // desired size is 10MB
	uint64_t iteration = 2;
	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert the big string
		DuckDB db(storage_database, config.get());
		Connection con(db);
		string big_string = string(string_length, 'a');
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a VARCHAR, j BIGINT);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES ('" + big_string + "', 1)"));
		while (string_length < desired_size) {
			REQUIRE_NO_FAIL(con.Query("INSERT INTO test SELECT a||a||a||a||a||a||a||a||a||a, " + to_string(iteration) +
			                          " FROM test"));
			REQUIRE_NO_FAIL(con.Query("DELETE FROM test WHERE j=" + to_string(iteration - 1)));
			iteration++;
			string_length *= 10;
		}

		// check the length
		result = con.Query("SELECT LENGTH(a) FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(string_length)}));
		result = con.Query("SELECT j FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(iteration - 1)}));
	}
	{
		DuckDB db(storage_database, config.get());
		Connection con(db);
		result = con.Query("SELECT LENGTH(a) FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(string_length)}));
		result = con.Query("SELECT j FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(iteration - 1)}));
	}
	// now reload the database, but this time with a max memory of 5MB
	{
		config->options.maximum_memory = 5000000;
		DuckDB db(storage_database, config.get());
		Connection con(db);
		// we can still select the integer
		result = con.Query("SELECT j FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(iteration - 1)}));
		// however the string is too big to fit in our buffer manager
		REQUIRE_FAIL(con.Query("SELECT LENGTH(a) FROM test"));
	}
	{
		// reloading with a bigger limit again makes it work
		config->options.maximum_memory = (idx_t)-1;
		DuckDB db(storage_database, config.get());
		Connection con(db);
		result = con.Query("SELECT LENGTH(a) FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(string_length)}));
		result = con.Query("SELECT j FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(iteration - 1)}));
	}
	DeleteDatabase(storage_database);
}

TEST_CASE("Test appending and checkpointing a table that exceeds buffer manager size", "[storage][.]") {
	duckdb::unique_ptr<MaterializedQueryResult> result;
	auto storage_database = TestCreatePath("storage_test");
	auto config = GetTestConfig();

	// maximum memory is 10MB
	config->options.force_compression = CompressionType::COMPRESSION_UNCOMPRESSED;
	config->options.maximum_memory = 10000000;
	config->options.maximum_threads = 1;

	// create a table of size 10 times the buffer pool size
	uint64_t size = 0, size_a, sum_a, sum_b;
	uint64_t table_size = 100000000 / sizeof(int32_t);
	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert the big string
		DuckDB db(storage_database, config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (1, 10), (2, 20), (3, 30), (NULL, NULL)"));
		size_a = 3;
		sum_a = 1 + 2 + 3;
		sum_b = 10 + 20 + 30;
		for (size = 4; size < table_size; size *= 2) {
			REQUIRE_NO_FAIL(con.Query("INSERT INTO test SELECT * FROM test"));
			size_a *= 2;
			sum_a *= 2;
			sum_b *= 2;
		}

		// check the aggregate statistics of the table
		result = con.Query("SELECT COUNT(*), COUNT(a), SUM(a), SUM(b) FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(size)}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(size_a)}));
		REQUIRE(CHECK_COLUMN(result, 2, {Value::BIGINT(sum_a)}));
		REQUIRE(CHECK_COLUMN(result, 3, {Value::BIGINT(sum_b)}));
	}
	for (idx_t i = 0; i < 2; i++) {
		// reload the table and checkpoint, still with a 10MB limit
		DuckDB db(storage_database, config.get());
		Connection con(db);

		result = con.Query("SELECT COUNT(*), COUNT(a), SUM(a), SUM(b) FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(size)}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(size_a)}));
		REQUIRE(CHECK_COLUMN(result, 2, {Value::BIGINT(sum_a)}));
		REQUIRE(CHECK_COLUMN(result, 3, {Value::BIGINT(sum_b)}));
	}
	DeleteDatabase(storage_database);
}

TEST_CASE("Modifying the buffer manager limit at runtime for an in-memory database", "[storage][.]") {
	duckdb::unique_ptr<MaterializedQueryResult> result;

	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA force_compression='uncompressed'"));

	// initialize an in-memory database of size 10MB
	uint64_t table_size = (1000 * 1000) / sizeof(int);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (1), (2), (3), (NULL)"));

	idx_t not_null_size = 3;
	idx_t size = 4;
	idx_t sum = 6;
	for (; size < table_size; size *= 2) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test SELECT * FROM test"));
		not_null_size *= 2;
		sum *= 2;
	}

	result = con.Query("SELECT COUNT(*), COUNT(a), SUM(a) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(size)}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(not_null_size)}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value::BIGINT(sum)}));

	// we can set the memory limit to 1GB
	REQUIRE_NO_FAIL(con.Query("PRAGMA memory_limit='1GB'"));
	// but we cannot set it below 10MB
	REQUIRE_FAIL(con.Query("PRAGMA memory_limit='1MB'"));

	// if we make room by dropping the table, we can set it to 1MB though
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA memory_limit='1MB'"));

	// also test that large strings are properly deleted
	// reset the memory limit
	REQUIRE_NO_FAIL(con.Query("PRAGMA memory_limit=-1"));

	// create a table with a large string (10MB)
	uint64_t string_length = 64;
	uint64_t desired_size = 10000000; // desired size is 10MB
	uint64_t iteration = 2;

	string big_string = string(string_length, 'a');
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a VARCHAR, j BIGINT);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES ('" + big_string + "', 1)"));
	while (string_length < desired_size) {
		REQUIRE_NO_FAIL(
		    con.Query("INSERT INTO test SELECT a||a||a||a||a||a||a||a||a||a, " + to_string(iteration) + " FROM test"));
		REQUIRE_NO_FAIL(con.Query("DELETE FROM test WHERE j=" + to_string(iteration - 1)));
		iteration++;
		string_length *= 10;
	}

	// now we cannot set the memory limit to 1MB again
	REQUIRE_FAIL(con.Query("PRAGMA memory_limit='1MB'"));
	// but dropping the table allows us to set the memory limit to 1MB again
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA memory_limit='1MB'"));
}

TEST_CASE("Test buffer reallocation", "[storage][.]") {
	auto storage_database = TestCreatePath("storage_test");
	auto config = GetTestConfig();
	// make sure the database does not exist
	DeleteDatabase(storage_database);
	DuckDB db(storage_database, config.get());

	// 1GB limit
	Connection con(db);
	const idx_t limit = 1000000000;
	REQUIRE_NO_FAIL(con.Query(StringUtil::Format("PRAGMA memory_limit='%lldB'", limit)));

	auto &buffer_manager = BufferManager::GetBufferManager(*con.context);
	CHECK(buffer_manager.GetUsedMemory() == 0);

	idx_t requested_size = Storage::BLOCK_SIZE;
	shared_ptr<BlockHandle> block;
	auto handle = buffer_manager.Allocate(requested_size, false, &block);
	CHECK(buffer_manager.GetUsedMemory() == BufferManager::GetAllocSize(requested_size));
	for (; requested_size < limit; requested_size *= 2) {
		// increase size
		buffer_manager.ReAllocate(block, requested_size);
		CHECK(buffer_manager.GetUsedMemory() == BufferManager::GetAllocSize(requested_size));
		// unpin and make sure it's evicted
		handle.Destroy();
		REQUIRE_NO_FAIL(con.Query(StringUtil::Format("PRAGMA memory_limit='%lldB'", requested_size)));
		CHECK(buffer_manager.GetUsedMemory() == 0);
		// re-pin
		REQUIRE_NO_FAIL(con.Query(StringUtil::Format("PRAGMA memory_limit='%lldB'", limit)));
		handle = buffer_manager.Pin(block);
		CHECK(buffer_manager.GetUsedMemory() == BufferManager::GetAllocSize(requested_size));
	}
	requested_size /= 2;
	for (; requested_size > Storage::BLOCK_SIZE; requested_size /= 2) {
		// decrease size
		buffer_manager.ReAllocate(block, requested_size);
		CHECK(buffer_manager.GetUsedMemory() == BufferManager::GetAllocSize(requested_size));
		// unpin and make sure it's evicted
		handle.Destroy();
		REQUIRE_NO_FAIL(con.Query(StringUtil::Format("PRAGMA memory_limit='%lldB'", requested_size)));
		CHECK(buffer_manager.GetUsedMemory() == 0);
		// re-pin
		REQUIRE_NO_FAIL(con.Query(StringUtil::Format("PRAGMA memory_limit='%lldB'", limit)));
		handle = buffer_manager.Pin(block);
		CHECK(buffer_manager.GetUsedMemory() == BufferManager::GetAllocSize(requested_size));
	}
}

TEST_CASE("Test buffer manager variable size allocations", "[storage][.]") {
	auto storage_database = TestCreatePath("storage_test");
	auto config = GetTestConfig();
	// make sure the database does not exist
	DeleteDatabase(storage_database);
	DuckDB db(storage_database, config.get());
	Connection con(db);

	auto &buffer_manager = BufferManager::GetBufferManager(*con.context);
	CHECK(buffer_manager.GetUsedMemory() == 0);

	idx_t requested_size = 424242;
	shared_ptr<BlockHandle> block;
	auto pin = buffer_manager.Allocate(requested_size, false, &block);
	CHECK(buffer_manager.GetUsedMemory() >= requested_size + Storage::BLOCK_HEADER_SIZE);

	pin.Destroy();
	block.reset();
	CHECK(buffer_manager.GetUsedMemory() == 0);
}

TEST_CASE("Test buffer manager buffer re-use", "[storage][.]") {
	auto storage_database = TestCreatePath("storage_test");
	auto config = GetTestConfig();
	// make sure the database does not exist
	DeleteDatabase(storage_database);
	DuckDB db(storage_database, config.get());
	Connection con(db);

	auto &buffer_manager = BufferManager::GetBufferManager(*con.context);
	CHECK(buffer_manager.GetUsedMemory() == 0);

	// Set memory limit to hold exactly 10 blocks
	idx_t pin_count = 10;
	REQUIRE_NO_FAIL(
	    con.Query(StringUtil::Format("PRAGMA memory_limit='%lldB'", Storage::BLOCK_ALLOC_SIZE * pin_count)));

	// Create 40 blocks, but don't hold the pin
	// They will be added to the eviction queue and the buffers will be re-used
	idx_t block_count = 40;
	duckdb::vector<shared_ptr<BlockHandle>> blocks;
	blocks.reserve(block_count);
	for (idx_t i = 0; i < block_count; i++) {
		blocks.emplace_back();
		buffer_manager.Allocate(Storage::BLOCK_SIZE, false, &blocks.back());
		// used memory should increment by exactly one block at a time, up to 10
		CHECK(buffer_manager.GetUsedMemory() == MinValue<idx_t>(pin_count, i + 1) * Storage::BLOCK_ALLOC_SIZE);
	}

	// now pin them one by one - cycling through should trigger more buffer re-use
	for (idx_t i = 0; i < block_count; i++) {
		auto pin = buffer_manager.Pin(blocks[i]);
		CHECK(buffer_manager.GetUsedMemory() == pin_count * Storage::BLOCK_ALLOC_SIZE);
	}

	// Clear all blocks and verify we go back down to 0 used memory
	blocks.clear();
	CHECK(buffer_manager.GetUsedMemory() == 0);

	// now we do exactly the same, but with variable-sized blocks
	idx_t block_size = 424242;
	idx_t alloc_size = BufferManager::GetAllocSize(block_size);
	REQUIRE_NO_FAIL(con.Query(StringUtil::Format("PRAGMA memory_limit='%lldB'", alloc_size * pin_count)));
	for (idx_t i = 0; i < block_count; i++) {
		blocks.emplace_back();
		buffer_manager.Allocate(block_size, false, &blocks.back());
		CHECK(buffer_manager.GetUsedMemory() == MinValue<idx_t>(pin_count, i + 1) * alloc_size);
	}
	for (idx_t i = 0; i < block_count; i++) {
		auto pin = buffer_manager.Pin(blocks[i]);
		CHECK(buffer_manager.GetUsedMemory() == pin_count * alloc_size);
	}
	blocks.clear();
	CHECK(buffer_manager.GetUsedMemory() == 0);

	// again, the same but incrementing block_size by 1 for every block (has same alloc_size)
	for (idx_t i = 0; i < block_count; i++) {
		blocks.emplace_back();
		buffer_manager.Allocate(block_size, false, &blocks.back());
		CHECK(buffer_manager.GetUsedMemory() == MinValue<idx_t>(pin_count, i + 1) * alloc_size);
		// increment block_size
		block_size++;
		CHECK(BufferManager::GetAllocSize(block_size) == alloc_size);
	}
	for (idx_t i = 0; i < block_count; i++) {
		auto pin = buffer_manager.Pin(blocks[i]);
		CHECK(buffer_manager.GetUsedMemory() == pin_count * alloc_size);
	}
	blocks.clear();
	CHECK(buffer_manager.GetUsedMemory() == 0);

	// reset block size and do the same but decrement by 1 for every block (still same alloc_size)
	block_size = 424242;
	for (idx_t i = 0; i < block_count; i++) {
		blocks.emplace_back();
		buffer_manager.Allocate(block_size, false, &blocks.back());
		CHECK(buffer_manager.GetUsedMemory() == MinValue<idx_t>(pin_count, i + 1) * alloc_size);
		// increment block_size
		block_size--;
		CHECK(BufferManager::GetAllocSize(block_size) == alloc_size);
	}
	for (idx_t i = 0; i < block_count; i++) {
		auto pin = buffer_manager.Pin(blocks[i]);
		CHECK(buffer_manager.GetUsedMemory() == pin_count * alloc_size);
	}
	blocks.clear();
	CHECK(buffer_manager.GetUsedMemory() == 0);
}

TEST_CASE("Test buffer allocator", "[storage][.]") {
	auto storage_database = TestCreatePath("storage_test");
	auto config = GetTestConfig();
	// make sure the database does not exist
	DeleteDatabase(storage_database);
	DuckDB db(storage_database, config.get());
	Connection con(db);

	auto &buffer_manager = BufferManager::GetBufferManager(*con.context);
	CHECK(buffer_manager.GetUsedMemory() == 0);

	const idx_t limit = 1000000000;
	REQUIRE_NO_FAIL(con.Query(StringUtil::Format("PRAGMA memory_limit='%lldB'", limit)));

	auto &allocator = buffer_manager.GetBufferAllocator();
	idx_t requested_size = Storage::BLOCK_SIZE;
	auto pointer = allocator.AllocateData(requested_size);
	idx_t current_size = requested_size;
	CHECK(buffer_manager.GetUsedMemory() == requested_size);

	// increase
	for (; requested_size < limit; requested_size *= 2) {
		pointer = allocator.ReallocateData(pointer, current_size, requested_size);
		current_size = requested_size;
		CHECK(buffer_manager.GetUsedMemory() == requested_size);
	}

	// decrease
	for (; requested_size >= Storage::BLOCK_SIZE; requested_size /= 2) {
		pointer = allocator.ReallocateData(pointer, current_size, requested_size);
		current_size = requested_size;
		CHECK(buffer_manager.GetUsedMemory() == requested_size);
	}

	allocator.FreeData(pointer, current_size);
}
