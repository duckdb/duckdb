#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/storage/data_table.hpp"

using namespace duckdb;

// All tests use sequential inserts into fresh tables, so row_id == insertion order (0, 1, 2, ...).

static ART &GetPKIndex(Connection &con, const string &table_name) {
	auto &context = *con.context;
	auto &catalog = Catalog::GetCatalog(context, INVALID_CATALOG);
	auto &table = catalog.GetEntry<TableCatalogEntry>(context, DEFAULT_SCHEMA, table_name);
	auto &storage = table.GetStorage();
	auto &indexes = storage.GetDataTableInfo()->GetIndexes();
	ART *result = nullptr;
	for (auto &index : indexes.Indexes()) {
		if (index.IsPrimary() && index.IsBound()) {
			result = &index.Cast<ART>();
			break;
		}
	}
	REQUIRE(result);
	return *result;
}

TEST_CASE("ART Scan batch - basic lookups", "[art]") {
#if STANDARD_VECTOR_SIZE < 4
	return;
#endif
	DuckDB db(nullptr);
	Connection con(db);

	// Create table with PK and insert 100 rows
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t SELECT i, i * 10 FROM range(100) tbl(i)"));

	con.BeginTransaction();
	auto &art = GetPKIndex(con, "t");

	SECTION("finds existing keys") {
		DataChunk input;
		input.Initialize(Allocator::DefaultAllocator(), {LogicalType::INTEGER});
		auto data = FlatVector::GetDataMutable<int32_t>(input.data[0]);
		// Look up keys 10, 20, 30
		data[0] = 10;
		data[1] = 20;
		data[2] = 30;
		input.SetCardinality(3);

		duckdb::vector<row_t> row_ids;
		art.Scan(input, row_ids);

		REQUIRE(row_ids.size() == 3);
		set<row_t> found(row_ids.begin(), row_ids.end());
		REQUIRE(found.count(10) == 1);
		REQUIRE(found.count(20) == 1);
		REQUIRE(found.count(30) == 1);
	}

	SECTION("missing keys are skipped") {
		DataChunk input;
		input.Initialize(Allocator::DefaultAllocator(), {LogicalType::INTEGER});
		auto data = FlatVector::GetDataMutable<int32_t>(input.data[0]);
		// 5 exists, 999 does not, 50 exists
		data[0] = 5;
		data[1] = 999;
		data[2] = 50;
		input.SetCardinality(3);

		duckdb::vector<row_t> row_ids;
		art.Scan(input, row_ids);

		REQUIRE(row_ids.size() == 2);
		set<row_t> found(row_ids.begin(), row_ids.end());
		REQUIRE(found.count(5) == 1);
		REQUIRE(found.count(50) == 1);
	}

	SECTION("empty input returns empty results") {
		DataChunk input;
		input.Initialize(Allocator::DefaultAllocator(), {LogicalType::INTEGER});
		input.SetCardinality(0);

		duckdb::vector<row_t> row_ids;
		art.Scan(input, row_ids);

		REQUIRE(row_ids.empty());
	}

	SECTION("all keys missing returns empty results") {
		DataChunk input;
		input.Initialize(Allocator::DefaultAllocator(), {LogicalType::INTEGER});
		auto data = FlatVector::GetDataMutable<int32_t>(input.data[0]);
		data[0] = 200;
		data[1] = 300;
		input.SetCardinality(2);

		duckdb::vector<row_t> row_ids;
		art.Scan(input, row_ids);

		REQUIRE(row_ids.empty());
	}

	SECTION("full vector size batch") {
		DataChunk input;
		input.Initialize(Allocator::DefaultAllocator(), {LogicalType::INTEGER});
		auto data = FlatVector::GetDataMutable<int32_t>(input.data[0]);
		// Fill with keys 0..99 (all exist)
		idx_t count = MinValue<idx_t>(100, STANDARD_VECTOR_SIZE);
		for (idx_t i = 0; i < count; i++) {
			data[i] = static_cast<int32_t>(i);
		}
		input.SetCardinality(count);

		duckdb::vector<row_t> row_ids;
		art.Scan(input, row_ids);

		REQUIRE(row_ids.size() == count);
		for (idx_t i = 0; i < count; i++) {
			REQUIRE(row_ids[i] == static_cast<row_t>(i));
		}
	}
}

TEST_CASE("ART Scan batch - max capacity", "[art]") {
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t_big(id BIGINT PRIMARY KEY)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t_big SELECT i FROM range(5000) tbl(i)"));

	con.BeginTransaction();
	auto &art = GetPKIndex(con, "t_big");

	DataChunk input;
	input.Initialize(Allocator::DefaultAllocator(), {LogicalType::BIGINT});
	auto data = FlatVector::GetDataMutable<int64_t>(input.data[0]);

	// Lookup a batch of 2048 keys (full STANDARD_VECTOR_SIZE)
	idx_t count = MinValue<idx_t>(2048, STANDARD_VECTOR_SIZE);
	for (idx_t i = 0; i < count; i++) {
		data[i] = static_cast<int64_t>(i * 2); // even numbers 0,2,4,...,4094
	}
	input.SetCardinality(count);

	duckdb::vector<row_t> row_ids;
	art.Scan(input, row_ids);

	REQUIRE(row_ids.size() == count);
	for (idx_t i = 0; i < count; i++) {
		REQUIRE(row_ids[i] == static_cast<row_t>(i * 2));
	}
}

TEST_CASE("ART Scan batch - VARCHAR keys", "[art]") {
#if STANDARD_VECTOR_SIZE < 4
	return;
#endif
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t_str(id VARCHAR PRIMARY KEY)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t_str VALUES ('alpha'), ('beta'), ('gamma'), ('delta'), "
	                          "('a_very_long_string_that_exceeds_inline_storage_for_testing_purposes')"));

	con.BeginTransaction();
	auto &art = GetPKIndex(con, "t_str");

	DataChunk input;
	input.Initialize(Allocator::DefaultAllocator(), {LogicalType::VARCHAR});

	// Look up 3 keys: 2 exist, 1 doesn't
	input.data[0].SetValue(0, Value("beta"));
	input.data[0].SetValue(1, Value("missing"));
	input.data[0].SetValue(2, Value("a_very_long_string_that_exceeds_inline_storage_for_testing_purposes"));
	input.SetCardinality(3);

	duckdb::vector<row_t> row_ids;
	art.Scan(input, row_ids);

	REQUIRE(row_ids.size() == 2);
	// Rows: alpha=0, beta=1, gamma=2, delta=3, long_string=4
	set<row_t> found(row_ids.begin(), row_ids.end());
	REQUIRE(found.count(1) == 1);
	REQUIRE(found.count(4) == 1);
}

TEST_CASE("ART Scan batch - composite key", "[art]") {
#if STANDARD_VECTOR_SIZE < 4
	return;
#endif
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t_comp(a INTEGER, b INTEGER, PRIMARY KEY(a, b))"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t_comp VALUES (1, 10), (1, 20), (2, 10), (2, 20), (3, 30)"));

	con.BeginTransaction();
	auto &art = GetPKIndex(con, "t_comp");

	DataChunk input;
	input.Initialize(Allocator::DefaultAllocator(), {LogicalType::INTEGER, LogicalType::INTEGER});
	auto col_a = FlatVector::GetDataMutable<int32_t>(input.data[0]);
	auto col_b = FlatVector::GetDataMutable<int32_t>(input.data[1]);

	// (1,10) exists, (1,30) doesn't, (2,20) exists, (9,9) doesn't
	col_a[0] = 1;
	col_b[0] = 10;
	col_a[1] = 1;
	col_b[1] = 30;
	col_a[2] = 2;
	col_b[2] = 20;
	col_a[3] = 9;
	col_b[3] = 9;
	input.SetCardinality(4);

	duckdb::vector<row_t> row_ids;
	art.Scan(input, row_ids);

	REQUIRE(row_ids.size() == 2);
	// Rows: (1,10)=0, (1,20)=1, (2,10)=2, (2,20)=3, (3,30)=4
	set<row_t> found(row_ids.begin(), row_ids.end());
	REQUIRE(found.count(0) == 1);
	REQUIRE(found.count(3) == 1);
}

TEST_CASE("ART Scan batch - non-unique index", "[art]") {
#if STANDARD_VECTOR_SIZE < 4
	return;
#endif
	DuckDB db(nullptr);
	Connection con(db);

	// Create table with a non-unique index (duplicate key values allowed)
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t_dup(id INTEGER, val INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t_dup VALUES (1, 10), (1, 20), (1, 30), (2, 40), (3, 50), (3, 60)"));
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX idx_dup ON t_dup(id)"));

	con.BeginTransaction();
	auto &context = *con.context;
	auto &catalog = Catalog::GetCatalog(context, INVALID_CATALOG);
	auto &table = catalog.GetEntry<TableCatalogEntry>(context, DEFAULT_SCHEMA, "t_dup");
	auto &storage = table.GetStorage();
	auto &indexes = storage.GetDataTableInfo()->GetIndexes();
	ART *art = nullptr;
	for (auto &index : indexes.Indexes()) {
		if (index.IsBound() && index.GetIndexName() == "idx_dup") {
			art = &index.Cast<ART>();
			break;
		}
	}
	REQUIRE(art);

	DataChunk input;
	input.Initialize(Allocator::DefaultAllocator(), {LogicalType::INTEGER});
	auto data = FlatVector::GetDataMutable<int32_t>(input.data[0]);

	// Key 1 has 3 rows, key 3 has 2 rows, key 999 doesn't exist
	data[0] = 1;
	data[1] = 3;
	data[2] = 999;
	input.SetCardinality(3);

	duckdb::vector<row_t> row_ids;
	art->Scan(input, row_ids);

	// Should return 5 row_ids total (3 for key=1, 2 for key=3, 0 for key=999)
	REQUIRE(row_ids.size() == 5);
	// Rows: (1,10)=0, (1,20)=1, (1,30)=2, (2,40)=3, (3,50)=4, (3,60)=5
	set<row_t> found(row_ids.begin(), row_ids.end());
	REQUIRE(found.count(0) == 1);
	REQUIRE(found.count(1) == 1);
	REQUIRE(found.count(2) == 1);
	REQUIRE(found.count(4) == 1);
	REQUIRE(found.count(5) == 1);
}

TEST_CASE("ART Scan batch - unique index", "[art]") {
#if STANDARD_VECTOR_SIZE < 4
	return;
#endif
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t_uniq(id INTEGER, val INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t_uniq VALUES (10, 1), (20, 2), (30, 3), (40, 4), (50, 5)"));
	REQUIRE_NO_FAIL(con.Query("CREATE UNIQUE INDEX idx_uniq ON t_uniq(id)"));

	con.BeginTransaction();
	auto &context = *con.context;
	auto &catalog = Catalog::GetCatalog(context, INVALID_CATALOG);
	auto &table = catalog.GetEntry<TableCatalogEntry>(context, DEFAULT_SCHEMA, "t_uniq");
	auto &storage = table.GetStorage();
	auto &indexes = storage.GetDataTableInfo()->GetIndexes();
	ART *art = nullptr;
	for (auto &index : indexes.Indexes()) {
		if (index.IsBound() && index.GetIndexName() == "idx_uniq") {
			art = &index.Cast<ART>();
			break;
		}
	}
	REQUIRE(art);

	DataChunk input;
	input.Initialize(Allocator::DefaultAllocator(), {LogicalType::INTEGER});
	auto data = FlatVector::GetDataMutable<int32_t>(input.data[0]);

	// 10 exists, 20 exists, 99 doesn't
	data[0] = 10;
	data[1] = 20;
	data[2] = 99;
	input.SetCardinality(3);

	duckdb::vector<row_t> row_ids;
	art->Scan(input, row_ids);

	// Unique index: at most 1 row_id per key, 2 found
	REQUIRE(row_ids.size() == 2);
	// Rows: (10,1)=0, (20,2)=1, (30,3)=2, (40,4)=3, (50,5)=4
	set<row_t> found(row_ids.begin(), row_ids.end());
	REQUIRE(found.count(0) == 1);
	REQUIRE(found.count(1) == 1);
}
