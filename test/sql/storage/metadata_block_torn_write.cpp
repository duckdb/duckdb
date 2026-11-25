#include "catch.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/storage/single_file_block_manager.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

static duckdb::vector<data_t> ReadDatabaseFile(FileSystem &fs, const string &path) {
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	auto size = NumericCast<idx_t>(handle->GetFileSize());
	duckdb::vector<data_t> result(size);
	if (size > 0) {
		handle->Read(QueryContext(), result.data(), size, 0);
	}
	return result;
}

TEST_CASE("Checkpoints must not rewrite metadata blocks the active header references", "[storage]") {
	// a crash can tear any in-place block rewrite - a block the current header references must stay
	// untouched until the next header is on disk, or the crashed file cannot be opened at all
	auto config = GetTestConfig();
	config->options.checkpoint_wal_size = idx_t(-1);
	config->SetOption(CheckpointOnShutdownSetting::SettingIndex, Value::BOOLEAN(false));

	LocalFileSystem fs;
	auto database_path = TestCreatePath("metadata_block_torn_write");
	DeleteDatabase(database_path);

	duckdb::vector<idx_t> metadata_block_ids;
	idx_t block_alloc_size = 0;
	{
		DuckDB db(database_path, config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers AS SELECT range i FROM range(1000)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));

		// the metadata blocks this checkpoint wrote - its header references all of them
		auto result = con.Query("SELECT DISTINCT block_id FROM pragma_metadata_info() ORDER BY block_id");
		REQUIRE_FALSE(result->HasError());
		for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
			metadata_block_ids.push_back(result->Collection().GetValue(0, row_idx).GetValue<idx_t>());
		}
		REQUIRE(!metadata_block_ids.empty());

		auto size_result = con.Query("SELECT block_size FROM pragma_database_size()");
		REQUIRE_FALSE(size_result->HasError());
		block_alloc_size = size_result->Collection().GetValue(0, 0).GetValue<idx_t>();
		REQUIRE(block_alloc_size > 0);
	}
	auto checkpointed_file = ReadDatabaseFile(fs, database_path);

	{
		// abort the next checkpoint right after the metadata flush, before the header write - the
		// file now models a crash inside the checkpoint's window between those two writes
		DuckDB db(database_path, config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1000)"));
		REQUIRE_NO_FAIL(con.Query("SET debug_checkpoint_abort='after_free_list_write'"));
		REQUIRE_FAIL(con.Query("CHECKPOINT"));
	}
	auto aborted_file = ReadDatabaseFile(fs, database_path);

	// every metadata block of the completed checkpoint must be byte-identical: the aborted
	// checkpoint may only have written to blocks the on-disk header does not reference
	for (auto block_id : metadata_block_ids) {
		idx_t offset = SingleFileBlockManager::BLOCK_START + block_id * block_alloc_size;
		REQUIRE(offset + block_alloc_size <= checkpointed_file.size());
		REQUIRE(offset + block_alloc_size <= aborted_file.size());
		bool block_unchanged =
		    memcmp(checkpointed_file.data() + offset, aborted_file.data() + offset, block_alloc_size) == 0;
		REQUIRE(block_unchanged);
	}

	{
		// the file recovers: the old checkpoint loads and the WAL replays the insert
		DuckDB db(database_path, config.get());
		Connection con(db);
		auto result = con.Query("SELECT count(*) FROM integers");
		REQUIRE_FALSE(result->HasError());
		REQUIRE(CHECK_COLUMN(result, 0, {1001}));
	}

	DeleteDatabase(database_path);
}
