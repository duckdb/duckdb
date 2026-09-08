#include <duckdb/main/settings.hpp>

#include "catch.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "duckdb/common/enums/memory_tag.hpp"
#include "test_helpers.hpp"

#include <chrono>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <thread>

#if !defined(_WIN32) && !defined(WIN32)
#include <sys/wait.h>
#include <unistd.h>
#endif

using namespace duckdb;

static constexpr const char *TEST_TEMP_OWNER_PREFIX = "duckdb_temp_owner_v1_";
static constexpr const char *TEST_TEMP_OWNER_SUFFIX = ".lock";
static constexpr const char *TEST_TEMP_FILE_PREFIX = "duckdb_temp_v1_";

class TestNoLockFileSystem : public LocalFileSystem {
public:
	explicit TestNoLockFileSystem(string path_prefix_p) : path_prefix(std::move(path_prefix_p)) {
	}

public:
	string GetName() const override {
		return "TestNoLockFileSystem";
	}

	bool CanHandleFile(const string &path) override {
		return StringUtil::StartsWith(path, path_prefix);
	}

	duckdb::unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                        optional_ptr<FileOpener> opener = nullptr) override {
		if (flags.Lock() == FileLockType::NO_LOCK) {
			return LocalFileSystem::OpenFile(path, flags, opener);
		}
		auto unlocked_flags = FileOpenFlags(flags.GetFlagsInternal(), FileLockType::NO_LOCK, flags.Compression());
		auto handle = LocalFileSystem::OpenFile(path, unlocked_flags, opener);
		if (!handle) {
			return nullptr;
		}
		handle.reset();
		throw IOException("Test file system does not support file locks");
	}

private:
	string path_prefix;
};

class TestCleanupFileSystem : public LocalFileSystem {
public:
	explicit TestCleanupFileSystem(string path_prefix_p, bool fail_next_list_p = false)
	    : path_prefix(std::move(path_prefix_p)), fail_next_list(fail_next_list_p) {
	}

public:
	string GetName() const override {
		return "TestCleanupFileSystem";
	}

	bool CanHandleFile(const string &path) override {
		return StringUtil::StartsWith(path, path_prefix);
	}

	bool ListFilesExtended(const string &directory, const std::function<void(OpenFileInfo &)> &callback,
	                       optional_ptr<FileOpener> opener = nullptr) override {
		list_count++;
		if (fail_next_list) {
			fail_next_list = false;
			throw IOException("Injected orphan cleanup failure");
		}
		return LocalFileSystem::ListFilesExtended(directory, callback, opener);
	}

	idx_t GetListCount() const {
		return list_count;
	}

private:
	string path_prefix;
	bool fail_next_list;
	idx_t list_count = 0;
};

static string TestTempOwnerPath(FileSystem &fs, const string &directory, const string &owner_id) {
	return fs.JoinPath(directory, string(TEST_TEMP_OWNER_PREFIX) + owner_id + TEST_TEMP_OWNER_SUFFIX);
}

static string TestTempOwnerManifest(const string &owner_id) {
	return "DUCKDB_TEMP_OWNER\n1\n" + owner_id + "\n";
}

static string TestTempStoragePath(FileSystem &fs, const string &directory, const string &owner_id) {
	return fs.JoinPath(directory, string(TEST_TEMP_FILE_PREFIX) + owner_id + "_storage_DEFAULT-0.tmp");
}

static string TestTempBlockPath(FileSystem &fs, const string &directory, const string &owner_id) {
	return fs.JoinPath(directory, string(TEST_TEMP_FILE_PREFIX) + owner_id + "_block-1.block");
}

static void TestWriteFile(FileSystem &fs, const string &path, const string &contents = "dummy") {
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
	handle->Write(QueryContext(), const_cast<char *>(contents.data()), contents.size(), 0);
}

static duckdb::vector<string> TestGetTempOwnerIds(FileSystem &fs, const string &directory) {
	duckdb::vector<string> result;
	fs.ListFiles(directory, [&](const string &file_name, bool is_directory) {
		if (is_directory || !StringUtil::StartsWith(file_name, TEST_TEMP_OWNER_PREFIX) ||
		    !StringUtil::EndsWith(file_name, TEST_TEMP_OWNER_SUFFIX)) {
			return;
		}
		auto prefix_size = strlen(TEST_TEMP_OWNER_PREFIX);
		auto suffix_size = strlen(TEST_TEMP_OWNER_SUFFIX);
		result.push_back(file_name.substr(prefix_size, file_name.size() - prefix_size - suffix_size));
	});
	return result;
}

static duckdb::vector<string> TestGetTempStorageFiles(FileSystem &fs, const string &directory,
                                                      const string &owner_id) {
	duckdb::vector<string> result;
	auto prefix = string(TEST_TEMP_FILE_PREFIX) + owner_id + "_storage_";
	fs.ListFiles(directory, [&](const string &file_name, bool is_directory) {
		if (!is_directory && StringUtil::StartsWith(file_name, prefix) && StringUtil::EndsWith(file_name, ".tmp")) {
			result.push_back(fs.JoinPath(directory, file_name));
		}
	});
	return result;
}

static duckdb::vector<string> TestGetSpillFiles(FileSystem &fs, const string &directory) {
	duckdb::vector<string> result;
	fs.ListFiles(directory, [&](const string &file_name, bool is_directory) {
		if (!is_directory && StringUtil::StartsWith(file_name, TEST_TEMP_FILE_PREFIX)) {
			result.push_back(fs.JoinPath(directory, file_name));
		}
	});
	return result;
}

static void TestSetTempDirectory(Connection &con, const string &temp_directory) {
	auto result = con.Query("SET temp_directory='" + temp_directory + "'");
	if (result->HasError()) {
		result->ThrowError();
	}
}

static duckdb::shared_ptr<BlockHandle> TestSpillLargeTemporaryBlock(BufferManager &buffer_manager, int fill_byte) {
	auto pin = buffer_manager.Allocate(MemoryTag::EXTENSION, buffer_manager.GetBlockSize() + 1024, false);
	memset(pin.Ptr(), fill_byte, pin.GetFileBuffer().AllocSize());
	auto block = pin.GetBlockHandle();
	pin.Destroy();

	auto &memory = block->GetMemory();
	auto lock = memory.GetLock();
	memory.Unload(lock);
	if (!memory.IsUnloaded()) {
		throw InternalException("Expected the test temporary block to be unloaded");
	}
	return block;
}

static void TestAdvanceTemporaryBlockId(Connection &con) {
	auto &buffer_manager = BufferManager::GetBufferManager(*con.context);
	auto pin = buffer_manager.Allocate(MemoryTag::EXTENSION, buffer_manager.GetBlockSize() + 1024, true);
	auto block = pin.GetBlockHandle();
	pin.Destroy();
	block.reset();
}

static void TestInitializeTempDirectory(DuckDB &db, const string &temp_directory) {
	Connection con(db);
	TestSetTempDirectory(con, temp_directory);
	auto &buffer_manager = db.instance->GetBufferManager();
	auto grouped_buffer = buffer_manager.ConstructManagedBuffer(buffer_manager.GetBlockSize(),
	                                                            DEFAULT_BLOCK_HEADER_STORAGE_SIZE, nullptr);
	buffer_manager.WriteTemporaryBuffer(MemoryTag::BASE_TABLE, 2, *grouped_buffer);
	auto block_buffer = buffer_manager.ConstructManagedBuffer(buffer_manager.GetBlockAllocSize(),
	                                                          DEFAULT_BLOCK_HEADER_STORAGE_SIZE, nullptr);
	buffer_manager.WriteTemporaryBuffer(MemoryTag::BASE_TABLE, 1, *block_buffer);
}

struct RunningQueryTemporaryBlockState {
	bool WaitUntilBlocked(chrono::seconds timeout) {
		unique_lock<mutex> guard(lock);
		return cv.wait_for(guard, timeout, [&]() { return blocked; });
	}

	void Release() {
		lock_guard<mutex> guard(lock);
		released = true;
		cv.notify_all();
	}

	mutex lock;
	condition_variable cv;
	bool blocked = false;
	bool released = false;
	bool initialized = false;
	duckdb::shared_ptr<BlockHandle> block;
};

static RunningQueryTemporaryBlockState *running_query_temp_block_state;

struct RunningQueryStateGuard {
	explicit RunningQueryStateGuard(RunningQueryTemporaryBlockState &state_p) : state(state_p) {
		running_query_temp_block_state = &state;
	}

	~RunningQueryStateGuard() {
		running_query_temp_block_state = nullptr;
	}

	RunningQueryTemporaryBlockState &state;
};

struct RunningQueryThreadGuard {
	RunningQueryThreadGuard(RunningQueryTemporaryBlockState &state_p, thread &query_thread_p)
	    : state(state_p), query_thread(query_thread_p) {
	}

	~RunningQueryThreadGuard() {
		state.Release();
		if (query_thread.joinable()) {
			query_thread.join();
		}
		state.block.reset();
	}

	RunningQueryTemporaryBlockState &state;
	thread &query_thread;
};

static void RunningQueryTemporaryBlockProbe(DataChunk &input, ExpressionState &state, Vector &result) {
	result.Reference(input.data[0]);

	auto probe_state = running_query_temp_block_state;
	if (!probe_state) {
		return;
	}

	{
		lock_guard<mutex> guard(probe_state->lock);
		if (probe_state->initialized) {
			return;
		}
		probe_state->initialized = true;
	}

	auto &buffer_manager = BufferManager::GetBufferManager(state.GetContext());
	auto block = TestSpillLargeTemporaryBlock(buffer_manager, 0x42);
	{
		unique_lock<mutex> guard(probe_state->lock);
		probe_state->block = block;
		probe_state->blocked = true;
		probe_state->cv.notify_all();
		probe_state->cv.wait(guard, [&]() { return probe_state->released; });
	}

	auto reloaded = buffer_manager.Pin(block);
	reloaded.Destroy();
}

static void RegisterRunningQueryTemporaryBlockProbe(Connection &con) {
	ScalarFunction function("running_query_temp_block_probe", {LogicalType::BIGINT}, LogicalType::BIGINT,
	                        RunningQueryTemporaryBlockProbe, nullptr, nullptr, nullptr, nullptr, LogicalType::INVALID,
	                        FunctionStability::VOLATILE);
	CreateScalarFunctionInfo info(function);
	con.context->RegisterFunction(info);
}

TEST_CASE("Test storing a big string that exceeds buffer manager size", "[storage][.]") {
	duckdb::unique_ptr<MaterializedQueryResult> result;
	auto storage_database = TestCreatePath("storage_test");
	auto config = GetTestConfig();
	config->SetOptionByName("default_block_size", Value::UBIGINT(DEFAULT_BLOCK_ALLOC_SIZE));
	config->options.maximum_threads = 1;
	// ZSTD can store this in a smaller way, force uncompressed so the 5mb max test correctly fails
	config->SetOptionByName("force_compression", "uncompressed");

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
			REQUIRE_NO_FAIL(con.Query("INSERT INTO test SELECT repeat(a, 10), " + to_string(iteration) + " FROM test"));
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

TEST_CASE("Test cleanup of orphaned temporary files", "[storage][temp_directory]") {
	auto fs = FileSystem::CreateLocal();
	auto temp_directory = TestCreatePath("orphaned_temp_files");
	TestDeleteDirectory(temp_directory);
	TestCreateDirectory(temp_directory);

	const string stale_owner_id = "11111111-1111-4111-8111-111111111111";
	const string second_stale_owner_id = "33333333-3333-4333-8333-333333333333";
	const string invalid_owner_id = "22222222-2222-4222-8222-222222222222";
	auto stale_owner_file = TestTempOwnerPath(*fs, temp_directory, stale_owner_id);
	auto stale_storage_file = TestTempStoragePath(*fs, temp_directory, stale_owner_id);
	auto stale_block_file = TestTempBlockPath(*fs, temp_directory, stale_owner_id);
	auto second_stale_owner_file = TestTempOwnerPath(*fs, temp_directory, second_stale_owner_id);
	auto second_stale_storage_file = TestTempStoragePath(*fs, temp_directory, second_stale_owner_id);
	auto stale_unrecognized_file =
	    fs->JoinPath(temp_directory, string(TEST_TEMP_FILE_PREFIX) + stale_owner_id + "_storage_DEFAULT-0.tmp.backup");
	auto invalid_owner_file = TestTempOwnerPath(*fs, temp_directory, invalid_owner_id);
	auto invalid_owned_file = TestTempStoragePath(*fs, temp_directory, invalid_owner_id);
	auto legacy_storage_file = fs->JoinPath(temp_directory, "duckdb_temp_storage_orphan.tmp");
	auto legacy_block_file = fs->JoinPath(temp_directory, "duckdb_temp_block-1.block");
	auto unrelated_file = fs->JoinPath(temp_directory, "unrelated_file.tmp");
	TestWriteFile(*fs, stale_owner_file, TestTempOwnerManifest(stale_owner_id));
	TestWriteFile(*fs, second_stale_owner_file, TestTempOwnerManifest(second_stale_owner_id));
	for (const auto &path : {stale_storage_file, stale_block_file, stale_unrecognized_file, invalid_owned_file,
	                         second_stale_storage_file, legacy_storage_file, legacy_block_file, unrelated_file}) {
		TestWriteFile(*fs, path);
	}
	TestWriteFile(*fs, invalid_owner_file, "DUCKDB_TEMP_OWNER\n2\n" + invalid_owner_id + "\n");

	{
		DuckDB db(nullptr);
		auto cleanup_fs = make_uniq<TestCleanupFileSystem>(temp_directory);
		auto cleanup_fs_ptr = cleanup_fs.get();
		db.GetFileSystem().RegisterSubSystem(std::move(cleanup_fs));
		TestInitializeTempDirectory(db, temp_directory);
		const idx_t expected_cleanup_scans = 2;
		REQUIRE(cleanup_fs_ptr->GetListCount() == expected_cleanup_scans);
		REQUIRE(!fs->FileExists(stale_owner_file));
		REQUIRE(!fs->FileExists(stale_storage_file));
		REQUIRE(!fs->FileExists(stale_block_file));
		REQUIRE(!fs->FileExists(second_stale_owner_file));
		REQUIRE(!fs->FileExists(second_stale_storage_file));
		REQUIRE(fs->FileExists(stale_unrecognized_file));
		REQUIRE(fs->FileExists(invalid_owner_file));
		REQUIRE(fs->FileExists(invalid_owned_file));
		REQUIRE(fs->FileExists(legacy_storage_file));
		REQUIRE(fs->FileExists(legacy_block_file));
		REQUIRE(fs->FileExists(unrelated_file));
	}

	TestDeleteDirectory(temp_directory);
}

TEST_CASE("Test cleanup skips live temporary directory owners", "[storage][temp_directory]") {
	auto fs = FileSystem::CreateLocal();
	auto temp_directory = TestCreatePath("live_temp_file_owner");
	TestDeleteDirectory(temp_directory);
	TestCreateDirectory(temp_directory);

	auto live_db = make_uniq<DuckDB>(nullptr);
	TestInitializeTempDirectory(*live_db, temp_directory);
	auto owner_ids = TestGetTempOwnerIds(*fs, temp_directory);
	REQUIRE(owner_ids.size() == 1);
	auto live_owner_file = TestTempOwnerPath(*fs, temp_directory, owner_ids[0]);
	auto live_storage_files = TestGetTempStorageFiles(*fs, temp_directory, owner_ids[0]);
	REQUIRE(live_storage_files.size() == 1);
	auto live_storage_file = live_storage_files[0];
	auto live_block_file = TestTempBlockPath(*fs, temp_directory, owner_ids[0]);
	REQUIRE(fs->FileExists(live_block_file));

	{
		DuckDB cleanup_db(nullptr);
		TestInitializeTempDirectory(cleanup_db, temp_directory);
		REQUIRE(fs->FileExists(live_owner_file));
		REQUIRE(fs->FileExists(live_storage_file));
		REQUIRE(fs->FileExists(live_block_file));
	}

	REQUIRE(fs->FileExists(live_owner_file));
	REQUIRE(fs->FileExists(live_storage_file));
	REQUIRE(fs->FileExists(live_block_file));
	live_db.reset();
	REQUIRE(!fs->FileExists(live_owner_file));
	REQUIRE(!fs->FileExists(live_storage_file));
	REQUIRE(!fs->FileExists(live_block_file));

	TestDeleteDirectory(temp_directory);
}

TEST_CASE("Test temporary files remain usable when owner locking is unavailable", "[storage][temp_directory]") {
	auto local_fs = FileSystem::CreateLocal();
	auto temp_directory = TestCreatePath("temp_file_owner_lock_unsupported");
	TestDeleteDirectory(temp_directory);
	TestCreateDirectory(temp_directory);

	{
		DuckDB db(nullptr);
		db.GetFileSystem().RegisterSubSystem(make_uniq<TestNoLockFileSystem>(temp_directory));
		TestInitializeTempDirectory(db, temp_directory);
		REQUIRE(TestGetTempOwnerIds(*local_fs, temp_directory).empty());
		REQUIRE(TestGetSpillFiles(*local_fs, temp_directory).size() == 2);
	}

	REQUIRE(TestGetTempOwnerIds(*local_fs, temp_directory).empty());
	REQUIRE(TestGetSpillFiles(*local_fs, temp_directory).empty());
	TestDeleteDirectory(temp_directory);
}

TEST_CASE("Test temporary files remain usable when orphan cleanup fails", "[storage][temp_directory]") {
	auto local_fs = FileSystem::CreateLocal();
	auto temp_directory = TestCreatePath("temp_file_cleanup_failure");
	TestDeleteDirectory(temp_directory);
	TestCreateDirectory(temp_directory);

	{
		DuckDB db(nullptr);
		db.GetFileSystem().RegisterSubSystem(make_uniq<TestCleanupFileSystem>(temp_directory, true));
		TestInitializeTempDirectory(db, temp_directory);
		REQUIRE(TestGetTempOwnerIds(*local_fs, temp_directory).size() == 1);
		REQUIRE(TestGetSpillFiles(*local_fs, temp_directory).size() == 2);
	}

	REQUIRE(TestGetTempOwnerIds(*local_fs, temp_directory).empty());
	REQUIRE(TestGetSpillFiles(*local_fs, temp_directory).empty());
	TestDeleteDirectory(temp_directory);
}

TEST_CASE("Running query keeps temporary files isolated from another DuckDB instance cleanup",
          "[storage][temp_directory]") {
	auto temp_directory = TestCreatePath("running_query_shared_temp_directory");
	TestDeleteDirectory(temp_directory);
	TestCreateDirectory(temp_directory);

	{
		RunningQueryTemporaryBlockState probe_state;
		RunningQueryStateGuard state_guard(probe_state);

		DuckDB db_a(nullptr);
		Connection con_a(db_a);
		TestSetTempDirectory(con_a, temp_directory);
		RegisterRunningQueryTemporaryBlockProbe(con_a);

		duckdb::unique_ptr<QueryResult> result;
		thread query_thread(
		    [&]() { result = con_a.Query("SELECT running_query_temp_block_probe(i) FROM range(1) t(i)"); });
		RunningQueryThreadGuard query_guard(probe_state, query_thread);

		if (!probe_state.WaitUntilBlocked(chrono::seconds(10))) {
			FAIL("Timed out waiting for the running query to spill its temporary block");
		}

		{
			DuckDB db_b(nullptr);
			Connection con_b(db_b);
			TestSetTempDirectory(con_b, temp_directory);
			// Keep B's spilled file at a different name so this exercises directory cleanup, not same-name removal.
			TestAdvanceTemporaryBlockId(con_b);
			auto &buffer_manager = BufferManager::GetBufferManager(*con_b.context);
			auto block_b = TestSpillLargeTemporaryBlock(buffer_manager, 0x5A);
			block_b.reset();
		}

		probe_state.Release();
		query_thread.join();

		REQUIRE(result);
		REQUIRE(NO_FAIL(*result));
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(0)}));
	}

	TestDeleteDirectory(temp_directory);
}

#if !defined(_WIN32) && !defined(WIN32)
TEST_CASE("Test cleanup skips temporary directory owners locked by another process",
          "[storage][temp_directory][.]") {
	auto fs = FileSystem::CreateLocal();
	auto temp_directory = TestCreatePath("live_temp_file_owner_process");
	TestDeleteDirectory(temp_directory);
	TestCreateDirectory(temp_directory);

	const string owner_id = "44444444-4444-4444-8444-444444444444";
	auto owner_file = TestTempOwnerPath(*fs, temp_directory, owner_id);
	auto storage_file = TestTempStoragePath(*fs, temp_directory, owner_id);
	auto owner_flags = FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_WRITE |
	                   FileFlags::FILE_FLAGS_FILE_CREATE_NEW | FileFlags::FILE_FLAGS_EXCLUSIVE_CREATE |
	                   FileLockType::WRITE_LOCK;
	auto owner_handle = fs->OpenFile(owner_file, owner_flags);
	auto manifest = TestTempOwnerManifest(owner_id);
	owner_handle->Write(QueryContext(), &manifest[0], manifest.size(), 0);
	fs->FileSync(*owner_handle);
	TestWriteFile(*fs, storage_file);

	auto pid = fork();
	REQUIRE(pid >= 0);
	if (pid == 0) {
		int exit_code = 0;
		try {
			{
				DuckDB cleanup_db(nullptr);
				TestInitializeTempDirectory(cleanup_db, temp_directory);
				auto child_fs = FileSystem::CreateLocal();
				if (!child_fs->FileExists(owner_file) || !child_fs->FileExists(storage_file)) {
					exit_code = 1;
				}
			}
		} catch (...) {
			exit_code = 2;
		}
		_exit(exit_code);
	}

	int child_status = 0;
	REQUIRE(waitpid(pid, &child_status, 0) == pid);
	REQUIRE(WIFEXITED(child_status));
	REQUIRE(WEXITSTATUS(child_status) == 0);
	REQUIRE(fs->FileExists(owner_file));
	REQUIRE(fs->FileExists(storage_file));

	owner_handle.reset();
	{
		DuckDB cleanup_db(nullptr);
		TestInitializeTempDirectory(cleanup_db, temp_directory);
	}
	REQUIRE(!fs->FileExists(owner_file));
	REQUIRE(!fs->FileExists(storage_file));

	TestDeleteDirectory(temp_directory);
}
#endif

TEST_CASE("Modifying the buffer manager limit at runtime for an in-memory database", "[storage][.]") {
	duckdb::unique_ptr<MaterializedQueryResult> result;

	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA force_compression='uncompressed'"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA temp_directory=''"));

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

TEST_CASE("Test buffer manager variable size allocations", "[storage][.]") {
	auto storage_database = TestCreatePath("storage_test");
	auto config = GetTestConfig();
	config->SetOptionByName("default_block_size", Value::UBIGINT(DEFAULT_BLOCK_ALLOC_SIZE));

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	DuckDB db(storage_database, config.get());
	Connection con(db);

	auto &buffer_manager = BufferManager::GetBufferManager(*con.context);
	CHECK(buffer_manager.GetUsedMemory() == 0);

	idx_t requested_size = 424242;
	auto pin = buffer_manager.Allocate(MemoryTag::EXTENSION, requested_size, false);
	auto block = pin.GetBlockHandle();
	CHECK(buffer_manager.GetUsedMemory() >= requested_size + block->GetBlockHeaderSize());

	pin.Destroy();
	block.reset();
	CHECK(buffer_manager.GetUsedMemory() == 0);
}

TEST_CASE("Test buffer manager buffer re-use", "[storage][.]") {
	auto storage_database = TestCreatePath("storage_test");
	auto config = GetTestConfig();
	config->SetOptionByName("default_block_size", Value::UBIGINT(DEFAULT_BLOCK_ALLOC_SIZE));

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	DuckDB db(storage_database, config.get());
	Connection con(db);

	auto &buffer_manager = BufferManager::GetBufferManager(*con.context);
	CHECK(buffer_manager.GetUsedMemory() == 0);

	// Set memory limit to hold exactly 10 blocks
	idx_t pin_count = 10;
	auto block_alloc_size = Settings::Get<DefaultBlockSizeSetting>(*config);
	auto block_size = block_alloc_size - Storage::DEFAULT_BLOCK_HEADER_SIZE;
	REQUIRE_NO_FAIL(con.Query(StringUtil::Format("PRAGMA memory_limit='%lldB'", block_alloc_size * pin_count)));

	// Create 40 blocks, but don't hold the pin
	// They will be added to the eviction queue and the buffers will be re-used
	idx_t block_count = 40;
	duckdb::vector<duckdb::shared_ptr<BlockHandle>> blocks;
	blocks.reserve(block_count);
	for (idx_t i = 0; i < block_count; i++) {
		auto pin = buffer_manager.Allocate(MemoryTag::EXTENSION, block_size, false);
		blocks.push_back(pin.GetBlockHandle());
		// used memory should increment by exactly one block at a time, up to 10
		CHECK(buffer_manager.GetUsedMemory() == MinValue<idx_t>(pin_count, i + 1) * block_alloc_size);
	}

	// now pin them one by one - cycling through should trigger more buffer re-use
	for (idx_t i = 0; i < block_count; i++) {
		auto pin = buffer_manager.Pin(blocks[i]);
		CHECK(buffer_manager.GetUsedMemory() == pin_count * block_alloc_size);
	}

	// Clear all blocks and verify we go back down to 0 used memory
	blocks.clear();
	CHECK(buffer_manager.GetUsedMemory() == 0);

	// now we do exactly the same, but with variable-sized blocks
	idx_t variable_block_size = 424242;
	auto alloc_size = BufferManager::GetAllocSize(variable_block_size + Storage::DEFAULT_BLOCK_HEADER_SIZE);
	REQUIRE_NO_FAIL(con.Query(StringUtil::Format("PRAGMA memory_limit='%lldB'", alloc_size * pin_count)));
	for (idx_t i = 0; i < block_count; i++) {
		auto pin = buffer_manager.Allocate(MemoryTag::EXTENSION, variable_block_size, false);
		blocks.push_back(pin.GetBlockHandle());
		CHECK(buffer_manager.GetUsedMemory() == MinValue<idx_t>(pin_count, i + 1) * alloc_size);
	}
	for (idx_t i = 0; i < block_count; i++) {
		auto pin = buffer_manager.Pin(blocks[i]);
		CHECK(buffer_manager.GetUsedMemory() == pin_count * alloc_size);
	}
	blocks.clear();
	CHECK(buffer_manager.GetUsedMemory() == 0);

	// again, the same but incrementing variable_block_size by 1 for every block (has same alloc_size)
	for (idx_t i = 0; i < block_count; i++) {
		auto pin = buffer_manager.Allocate(MemoryTag::EXTENSION, variable_block_size, false);
		blocks.push_back(pin.GetBlockHandle());
		CHECK(buffer_manager.GetUsedMemory() == MinValue<idx_t>(pin_count, i + 1) * alloc_size);
		// increment variable_block_size
		variable_block_size++;
		CHECK(BufferManager::GetAllocSize(variable_block_size + pin.GetBlockHandle()->GetBlockHeaderSize()) ==
		      alloc_size);
	}
	for (idx_t i = 0; i < block_count; i++) {
		auto pin = buffer_manager.Pin(blocks[i]);
		CHECK(buffer_manager.GetUsedMemory() == pin_count * alloc_size);
	}
	blocks.clear();
	CHECK(buffer_manager.GetUsedMemory() == 0);

	// reset block size and do the same but decrement by 1 for every block (still same alloc_size)
	variable_block_size = 424242;
	for (idx_t i = 0; i < block_count; i++) {
		auto pin = buffer_manager.Allocate(MemoryTag::EXTENSION, variable_block_size, false);
		blocks.push_back(pin.GetBlockHandle());
		CHECK(buffer_manager.GetUsedMemory() == MinValue<idx_t>(pin_count, i + 1) * alloc_size);
		// increment variable_block_size
		variable_block_size--;
		CHECK(BufferManager::GetAllocSize(variable_block_size + pin.GetBlockHandle()->GetBlockHeaderSize()) ==
		      alloc_size);
	}
	for (idx_t i = 0; i < block_count; i++) {
		auto pin = buffer_manager.Pin(blocks[i]);
		CHECK(buffer_manager.GetUsedMemory() == pin_count * alloc_size);
	}
	blocks.clear();
	CHECK(buffer_manager.GetUsedMemory() == 0);
}

TEST_CASE("Test evicted_data not double-decremented for variable-sized blocks", "[storage][.]") {
	auto storage_database = TestCreatePath("storage_test");
	auto config = GetTestConfig();
	config->SetOptionByName("default_block_size", Value::UBIGINT(DEFAULT_BLOCK_ALLOC_SIZE));
	config->options.maximum_threads = 1;
	DeleteDatabase(storage_database);
	DuckDB db(storage_database, config.get());
	Connection con(db);
	auto &buffer_manager = BufferManager::GetBufferManager(*con.context);

	idx_t variable_block_size = 424242;
	auto alloc_size = BufferManager::GetAllocSize(variable_block_size + Storage::DEFAULT_BLOCK_HEADER_SIZE);
	REQUIRE_NO_FAIL(con.Query(StringUtil::Format("PRAGMA memory_limit='%lldB'", alloc_size)));
	REQUIRE_NO_FAIL(con.Query("PRAGMA temp_directory='" + TestCreatePath("eviction_tracking_temp") + "'"));

	shared_ptr<BlockHandle> block_a = nullptr;
	shared_ptr<BlockHandle> block_b = nullptr;
	{
		auto pin = buffer_manager.Allocate(MemoryTag::EXTENSION, variable_block_size, false);
		block_a = pin.GetBlockHandle();
	}
	{
		auto pin = buffer_manager.Allocate(MemoryTag::EXTENSION, variable_block_size, false);
		block_b = pin.GetBlockHandle();
	}

	// Read block_a back from disk.
	{ auto pin = buffer_manager.Pin(block_a); }

	// Destroy both handles, so remaining temp files are cleaned up.
	block_a.reset();
	block_b.reset();

	// For now there should be no blocks on disk, and evicted_data must be 0.
	for (auto &entry : buffer_manager.GetMemoryUsageInfo()) {
		if (entry.tag == MemoryTag::EXTENSION) {
			CHECK(entry.evicted_data == 0);
		}
	}
}

TEST_CASE("Test buffer allocator", "[storage][.]") {
	auto storage_database = TestCreatePath("storage_test");
	auto config = GetTestConfig();
	config->SetOptionByName("default_block_size", Value::UBIGINT(DEFAULT_BLOCK_ALLOC_SIZE));

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	DuckDB db(storage_database, config.get());
	Connection con(db);

	auto &buffer_manager = BufferManager::GetBufferManager(*con.context);
	CHECK(buffer_manager.GetUsedMemory() == 0);

	const idx_t limit = 1000000000;
	REQUIRE_NO_FAIL(con.Query(StringUtil::Format("PRAGMA memory_limit='%lldB'", limit)));

	auto &allocator = buffer_manager.GetBufferAllocator();
	auto block_size = Settings::Get<DefaultBlockSizeSetting>(*config) - Storage::DEFAULT_BLOCK_HEADER_SIZE;
	idx_t requested_size = block_size;
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
	for (; requested_size >= block_size; requested_size /= 2) {
		pointer = allocator.ReallocateData(pointer, current_size, requested_size);
		current_size = requested_size;
		CHECK(buffer_manager.GetUsedMemory() == requested_size);
	}

	allocator.FreeData(pointer, current_size);
}
