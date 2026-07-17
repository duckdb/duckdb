#include "catch.hpp"
#include "duckdb/common/checksum.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

static void CorruptRoaringMetadataOffset(const string &path, block_id_t block_id, idx_t block_offset) {
	auto fs = FileSystem::CreateLocal();
	auto handle = fs->OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);
	auto block_location = Storage::FILE_HEADER_SIZE * 3 + NumericCast<idx_t>(block_id) * DEFAULT_BLOCK_ALLOC_SIZE;
	auto block_data = duckdb::unique_ptr<data_t[]>(new data_t[DEFAULT_BLOCK_ALLOC_SIZE]);

	handle->Read(QueryContext(), block_data.get(), DEFAULT_BLOCK_ALLOC_SIZE, block_location);

	REQUIRE(block_offset + sizeof(idx_t) < Storage::DEFAULT_BLOCK_SIZE);
	auto metadata_offset = Storage::DEFAULT_BLOCK_SIZE - block_offset - sizeof(idx_t) + 1;
	REQUIRE(metadata_offset < Storage::DEFAULT_BLOCK_SIZE);
	Store<idx_t>(metadata_offset, block_data.get() + DEFAULT_BLOCK_HEADER_STORAGE_SIZE + block_offset);

	auto checksum = Checksum(block_data.get() + DEFAULT_BLOCK_HEADER_STORAGE_SIZE, Storage::DEFAULT_BLOCK_SIZE);
	Store<uint64_t>(checksum, block_data.get());

	handle->Write(QueryContext(), block_data.get(), DEFAULT_BLOCK_ALLOC_SIZE, block_location);
	handle->Sync();
}

TEST_CASE("Roaring metadata offset cannot point outside block bounds", "[storage]") {
	auto config = GetTestConfig();
	config->SetOptionByName("default_block_size", Value::UBIGINT(DEFAULT_BLOCK_ALLOC_SIZE));
	auto storage_database = TestCreatePath("roaring_corrupt_metadata_offset");

	DeleteDatabase(storage_database);
	block_id_t block_id;
	idx_t block_offset;
	{
		DuckDB db(storage_database, config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("SET storage_compatibility_version='v1.2.0'"));
		REQUIRE_NO_FAIL(con.Query("SET force_compression='roaring'"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT CASE WHEN i % 3 = 0 THEN i::INTEGER ELSE NULL END AS v "
		                          "FROM range(100000) tbl(i)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
		auto result = con.Query("SELECT block_id, block_offset FROM pragma_storage_info('t') "
		                        "WHERE compression='Roaring' AND segment_type='VALIDITY'");
		REQUIRE(!result->HasError());
		REQUIRE(result->RowCount() == 1);
		block_id = result->GetValue(0, 0).GetValue<int64_t>();
		block_offset = NumericCast<idx_t>(result->GetValue(1, 0).GetValue<int64_t>());
	}

	CorruptRoaringMetadataOffset(storage_database, block_id, block_offset);

	{
		DuckDB db(storage_database, config.get());
		Connection con(db);
		auto result = con.Query("SELECT count(v) FROM t");
		REQUIRE(result->HasError());
		REQUIRE(StringUtil::Contains(result->GetError(), "Corrupted Roaring segment"));
	}
	DeleteDatabase(storage_database);
}
