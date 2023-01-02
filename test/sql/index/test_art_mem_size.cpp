#include "catch.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/client_context.hpp"

#include "duckdb/execution/index/art/prefix.hpp"
#include "duckdb/execution/index/art/leaf.hpp"
#include "duckdb/execution/index/art/node4.hpp"
#include "duckdb/execution/index/art/node16.hpp"
#include "duckdb/execution/index/art/node48.hpp"
#include "duckdb/execution/index/art/node256.hpp"

using namespace duckdb;

// TODO: I am manually setting some random byte values here because the sort code of the
// TODO: CREATE INDEX statement is 'leaking' some 8-ish bytes? I think I need to update this, inlining or something
// changed this

TEST_CASE("Test ART memory size", "[art][.]") {
	auto storage_database = TestCreatePath("storage_test");
	auto config = GetTestConfig();
	// make sure the database does not exist
	DeleteDatabase(storage_database);
	DuckDB db(storage_database, config.get());
	Connection con(db);

	// get some variables for calculations
	auto prefix_size = sizeof(Prefix);
	auto leaf_size = sizeof(Leaf);
	auto node4_size = sizeof(Node4);
	auto node16_size = sizeof(Node16);
	auto node48_size = sizeof(Node48);
	auto node256_size = sizeof(Node256);

	idx_t current_memory = 0;
	auto &buffer_manager = BufferManager::GetBufferManager(*con.context);
	REQUIRE(buffer_manager.GetUsedMemory() == current_memory);

	// test single leaf node with duplicates
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE leaf AS SELECT 42 AS id FROM range(42);"));
	current_memory = buffer_manager.GetUsedMemory();

	// TODO

	//	REQUIRE_NO_FAIL(con.Query("CREATE INDEX idx_leaf ON leaf(id);"));
	//	auto leaf_mem = prefix_size + 4 * sizeof(uint8_t) + leaf_size + 42 * sizeof(row_t);
	//	REQUIRE(buffer_manager.GetUsedMemory() - current_memory == leaf_mem);
	//
	//	// test Node4
	//	REQUIRE_NO_FAIL(con.Query("CREATE TABLE node4 AS SELECT range % 4 AS id FROM range(16);"));
	//	current_memory = buffer_manager.GetUsedMemory();
	//
	//	REQUIRE_NO_FAIL(con.Query("CREATE INDEX idx_node4 ON node4(id);"));
	//	auto node4_mem = prefix_size + 7 * sizeof(uint8_t) + node4_size;
	//	node4_mem += 4 * (prefix_size + leaf_size + 4 * sizeof(row_t));
	//	REQUIRE(buffer_manager.GetUsedMemory() - current_memory == node4_mem);
	//
	//	// test Node16
	//	REQUIRE_NO_FAIL(con.Query("CREATE TABLE node16 AS SELECT range % 16 AS id FROM range(32);"));
	//	current_memory = buffer_manager.GetUsedMemory();
	//
	//	REQUIRE_NO_FAIL(con.Query("CREATE INDEX idx_node16 ON node16(id);"));
	//	auto node16_mem = prefix_size + 7 * sizeof(uint8_t) + node16_size;
	//	node16_mem += 16 * (prefix_size + leaf_size + 2 * sizeof(row_t));
	//	REQUIRE(buffer_manager.GetUsedMemory() - current_memory == node16_mem);
	//
	//	// test Node48
	//	REQUIRE_NO_FAIL(con.Query("CREATE TABLE node48 AS SELECT range % 48 AS id FROM range(96);"));
	//	current_memory = buffer_manager.GetUsedMemory();
	//
	//	REQUIRE_NO_FAIL(con.Query("CREATE INDEX idx_node48 ON node48(id);"));
	//	auto node48_mem = prefix_size + 7 * sizeof(uint8_t) + node48_size;
	//	node48_mem += 48 * (prefix_size + leaf_size + 2 * sizeof(row_t));
	//	REQUIRE(buffer_manager.GetUsedMemory() - current_memory == node48_mem);
	//
	//	// test Node256
	//	REQUIRE_NO_FAIL(con.Query("CREATE TABLE node256 AS SELECT range % 256 AS id FROM range(512);"));
	//	current_memory = buffer_manager.GetUsedMemory();
	//
	//	REQUIRE_NO_FAIL(con.Query("CREATE INDEX idx_node256 ON node256(id);"));
	//	auto node256_mem = prefix_size + 7 * sizeof(uint8_t) + node256_size;
	//	node256_mem += 256 * (prefix_size + leaf_size + 2 * sizeof(row_t));
	//	REQUIRE(buffer_manager.GetUsedMemory() - current_memory == node256_mem);
}
