#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/extension_manager.hpp"
#include "test_helpers.hpp"

#include <thread>

using namespace duckdb;

namespace {

struct CopyAbortTestInfo : CopyFunctionInfo {
	CopyAbortTestInfo(bool fail_sink_p, bool fail_finalize_p = false, bool block_initialize_p = false)
	    : fail_sink(fail_sink_p), fail_finalize(fail_finalize_p), block_initialize(block_initialize_p) {
	}

	bool fail_sink;
	bool fail_finalize;
	bool block_initialize;
	atomic<idx_t> initialize_count {0};
	atomic<idx_t> finalize_count {0};
	atomic<idx_t> abort_count {0};
	atomic<bool> release_initialize {false};
};

struct CopyAbortBindData : FunctionData {
	explicit CopyAbortBindData(shared_ptr<CopyFunctionInfo> info_p) : info(std::move(info_p)) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<CopyAbortBindData>(info);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<CopyAbortBindData>();
		return info == other.info;
	}

	CopyAbortTestInfo &GetInfo() {
		return info->Cast<CopyAbortTestInfo>();
	}

	shared_ptr<CopyFunctionInfo> info;
};

struct CopyAbortLocalData : LocalFunctionData {};

struct CopyAbortGlobalData : GlobalFunctionData {};

struct CopyAbortPreparedData : PreparedBatchData {};

static unique_ptr<FunctionData> CopyAbortBind(ClientContext &, CopyFunctionBindInput &input, const vector<Identifier> &,
                                              const vector<LogicalType> &) {
	return make_uniq<CopyAbortBindData>(input.function_info);
}

static unique_ptr<LocalFunctionData> CopyAbortInitializeLocal(ExecutionContext &, FunctionData &) {
	return make_uniq<CopyAbortLocalData>();
}

static unique_ptr<GlobalFunctionData> CopyAbortInitializeGlobal(ClientContext &, FunctionData &bind_data,
                                                                const string &) {
	auto &info = bind_data.Cast<CopyAbortBindData>().GetInfo();
	++info.initialize_count;
	while (info.block_initialize && !info.release_initialize) {
		std::this_thread::yield();
	}
	return make_uniq<CopyAbortGlobalData>();
}

static void CopyAbortSink(ExecutionContext &, FunctionData &bind_data, GlobalFunctionData &, LocalFunctionData &,
                          DataChunk &) {
	if (bind_data.Cast<CopyAbortBindData>().GetInfo().fail_sink) {
		throw IOException("Injected COPY sink failure");
	}
}

static void CopyAbortCombine(ExecutionContext &, FunctionData &, GlobalFunctionData &, LocalFunctionData &) {
}

static void CopyAbortFinalize(ClientContext &, FunctionData &bind_data, GlobalFunctionData &) {
	auto &info = bind_data.Cast<CopyAbortBindData>().GetInfo();
	++info.finalize_count;
	if (info.fail_finalize) {
		throw IOException("Injected COPY finalize failure");
	}
}

static void CopyAbortAbort(ClientContext &, FunctionData &bind_data, GlobalFunctionData &) {
	++bind_data.Cast<CopyAbortBindData>().GetInfo().abort_count;
}

static CopyFunctionExecutionMode CopyAbortBatchExecutionMode(bool, bool) {
	return CopyFunctionExecutionMode::BATCH_COPY_TO_FILE;
}

static unique_ptr<PreparedBatchData> CopyAbortPrepareBatch(ClientContext &, FunctionData &, GlobalFunctionData &,
                                                           unique_ptr<ColumnDataCollection>) {
	return make_uniq<CopyAbortPreparedData>();
}

static void CopyAbortFlushBatch(ClientContext &, FunctionData &, GlobalFunctionData &, PreparedBatchData &) {
}

static CopyFunction CreateCopyAbortFunction(const string &name, const shared_ptr<CopyAbortTestInfo> &info) {
	CopyFunction function {Identifier(name)};
	function.copy_to_bind = CopyAbortBind;
	function.copy_to_initialize_local = CopyAbortInitializeLocal;
	function.copy_to_initialize_global = CopyAbortInitializeGlobal;
	function.copy_to_sink = CopyAbortSink;
	function.copy_to_combine = CopyAbortCombine;
	function.copy_to_finalize = CopyAbortFinalize;
	function.copy_to_abort = CopyAbortAbort;
	function.function_info = info;
	return function;
}

} // namespace

TEST_CASE("COPY dispatches abort only for unfinished file state", "[api][copy]") {
	DuckDB db(nullptr);
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("SET threads=1"));

	ExtensionInfo extension_info {};
	ExtensionActiveLoad load_info {*db.instance, extension_info, "copy_abort_test", ""};
	ExtensionLoader loader {load_info};

	auto failure_info = make_shared_ptr<CopyAbortTestInfo>(true);
	loader.RegisterFunction(CreateCopyAbortFunction("copy_abort_failure", failure_info));
	REQUIRE_FAIL(connection.Query(
	    "COPY (SELECT i FROM range(1) t(i)) TO 'copy_abort_failure.test' (FORMAT copy_abort_failure)"));
	REQUIRE(failure_info->initialize_count == 1);
	REQUIRE(failure_info->finalize_count == 0);
	REQUIRE(failure_info->abort_count == 1);

	auto finalize_failure_info = make_shared_ptr<CopyAbortTestInfo>(false, true);
	loader.RegisterFunction(CreateCopyAbortFunction("copy_abort_finalize_failure", finalize_failure_info));
	REQUIRE_FAIL(connection.Query("COPY (SELECT i FROM range(1) t(i)) TO 'copy_abort_finalize_failure.test' "
	                              "(FORMAT copy_abort_finalize_failure)"));
	REQUIRE(finalize_failure_info->initialize_count == 1);
	REQUIRE(finalize_failure_info->finalize_count == 1);
	REQUIRE(finalize_failure_info->abort_count == 1);

	auto success_info = make_shared_ptr<CopyAbortTestInfo>(false);
	loader.RegisterFunction(CreateCopyAbortFunction("copy_abort_success", success_info));
	REQUIRE_NO_FAIL(connection.Query(
	    "COPY (SELECT i FROM range(1) t(i)) TO 'copy_abort_success.test' (FORMAT copy_abort_success)"));
	REQUIRE(success_info->initialize_count == 1);
	REQUIRE(success_info->finalize_count == 1);
	REQUIRE(success_info->abort_count == 0);

	auto no_finalize_info = make_shared_ptr<CopyAbortTestInfo>(false);
	auto no_finalize = CreateCopyAbortFunction("copy_abort_no_finalize", no_finalize_info);
	no_finalize.copy_to_finalize = nullptr;
	loader.RegisterFunction(std::move(no_finalize));
	REQUIRE_NO_FAIL(connection.Query(
	    "COPY (SELECT i FROM range(1) t(i)) TO 'copy_abort_no_finalize.test' (FORMAT copy_abort_no_finalize)"));
	REQUIRE(no_finalize_info->initialize_count == 1);
	REQUIRE(no_finalize_info->finalize_count == 0);
	REQUIRE(no_finalize_info->abort_count == 0);

	auto batch_success_info = make_shared_ptr<CopyAbortTestInfo>(false);
	auto batch_success = CreateCopyAbortFunction("copy_abort_batch_success", batch_success_info);
	batch_success.copy_to_finalize = nullptr;
	batch_success.execution_mode = CopyAbortBatchExecutionMode;
	batch_success.prepare_batch = CopyAbortPrepareBatch;
	batch_success.flush_batch = CopyAbortFlushBatch;
	loader.RegisterFunction(std::move(batch_success));
	REQUIRE_NO_FAIL(connection.Query("SET threads=4"));
	REQUIRE_NO_FAIL(connection.Query("CREATE TABLE copy_abort_batch_table AS FROM range(4096)"));
	REQUIRE_NO_FAIL(connection.Query("COPY copy_abort_batch_table TO 'copy_abort_batch_success.test' "
	                                 "(FORMAT copy_abort_batch_success)"));
	REQUIRE(batch_success_info->initialize_count == 1);
	REQUIRE(batch_success_info->finalize_count == 0);
	REQUIRE(batch_success_info->abort_count == 0);
	REQUIRE_NO_FAIL(connection.Query("SET threads=1"));

	auto populated_directory = TestCreatePath("copy_abort_populated");
	auto local_fs = FileSystem::CreateLocal();
	if (local_fs->DirectoryExists(populated_directory)) {
		local_fs->RemoveDirectory(populated_directory);
	}
	auto populated_info = make_shared_ptr<CopyAbortTestInfo>(true, false, true);
	loader.RegisterFunction(CreateCopyAbortFunction("copy_abort_populated", populated_info));
	atomic<bool> query_succeeded {false};
	std::thread query_thread([&]() {
		auto result = connection.Query(StringUtil::Format(
		    "COPY (SELECT i FROM range(4096) t(i)) TO '%s' (FORMAT copy_abort_populated, PER_THREAD_OUTPUT true)",
		    populated_directory));
		query_succeeded = !result->HasError();
	});
	bool initialize_started = false;
	for (idx_t i = 0; i < 1000000; i++) {
		if (populated_info->initialize_count > 0) {
			initialize_started = true;
			break;
		}
		std::this_thread::yield();
	}
	auto keep_file = local_fs->JoinPath(populated_directory, "keep");
	if (initialize_started) {
		auto keep_handle =
		    local_fs->OpenFile(keep_file, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
	}
	populated_info->release_initialize = true;
	query_thread.join();
	REQUIRE(initialize_started);
	REQUIRE(!query_succeeded);
	REQUIRE(local_fs->DirectoryExists(populated_directory));
	REQUIRE(local_fs->FileExists(keep_file));
	local_fs->RemoveDirectory(populated_directory);
}

TEST_CASE("COPY cleans failed file and directory outputs", "[api][copy]") {
	DuckDB db(nullptr);
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("SET threads=1"));
	auto &fs = FileSystem::GetFileSystem(*connection.context);

	auto batch_path = TestCreatePath("copy_abort_batch.csv");
	fs.TryRemoveFile(batch_path);
	REQUIRE_FAIL(connection.Query(StringUtil::Format(
	    "COPY (SELECT CASE WHEN i=4096 THEN error('injected') ELSE i::VARCHAR END AS v FROM range(8192) t(i)) "
	    "TO '%s' (FORMAT CSV)",
	    batch_path)));
	REQUIRE(!fs.FileExists(batch_path));

	auto blob_path = TestCreatePath("copy_abort_regular.blob");
	fs.TryRemoveFile(blob_path);
	REQUIRE_FAIL(connection.Query(
	    StringUtil::Format("COPY (SELECT CASE WHEN i=4096 THEN error('injected') ELSE repeat('x', 100)::BLOB END AS v "
	                       "FROM range(8192) t(i)) TO '%s' (FORMAT BLOB)",
	                       blob_path)));
	REQUIRE(!fs.FileExists(blob_path));

	auto compressed_blob_path = TestCreatePath("copy_abort_regular.blob.gz");
	fs.TryRemoveFile(compressed_blob_path);
	REQUIRE_FAIL(connection.Query(
	    StringUtil::Format("COPY (SELECT CASE WHEN i=4096 THEN error('injected') ELSE repeat('x', 100)::BLOB END AS v "
	                       "FROM range(8192) t(i)) TO '%s' (FORMAT BLOB, COMPRESSION GZIP)",
	                       compressed_blob_path)));
	REQUIRE(!fs.FileExists(compressed_blob_path));

	auto parquet_path = TestCreatePath("copy_abort_regular.parquet");
	fs.TryRemoveFile(parquet_path);
	REQUIRE_FAIL(connection.Query(StringUtil::Format(
	    "COPY (SELECT CASE WHEN i=4096 THEN error('injected') ELSE i END AS v FROM range(8192) t(i)) "
	    "TO '%s' (FORMAT PARQUET, PRESERVE_ORDER false)",
	    parquet_path)));
	REQUIRE(!fs.FileExists(parquet_path));

	auto rotated_directory = TestCreatePath("copy_abort_rotated");
	if (fs.DirectoryExists(rotated_directory)) {
		fs.RemoveDirectory(rotated_directory);
	}
	REQUIRE_FAIL(connection.Query(
	    StringUtil::Format("COPY (SELECT CASE WHEN i=8192 THEN error('injected') ELSE repeat('x', 100) END AS v "
	                       "FROM range(16384) t(i)) TO '%s' "
	                       "(FORMAT CSV, FILE_SIZE_BYTES '16KB', PRESERVE_ORDER false)",
	                       rotated_directory)));
	REQUIRE(!fs.DirectoryExists(rotated_directory));

	auto partitioned_directory = TestCreatePath("copy_abort_partitioned");
	if (fs.DirectoryExists(partitioned_directory)) {
		fs.RemoveDirectory(partitioned_directory);
	}
	REQUIRE_FAIL(connection.Query(
	    StringUtil::Format("COPY (SELECT i %% 4 AS p, CASE WHEN i=16384 THEN error('injected') "
	                       "ELSE repeat('x', 100) END AS v FROM range(32768) t(i)) TO '%s' "
	                       "(FORMAT CSV, PARTITION_BY (p), FILE_SIZE_BYTES '16KB', PRESERVE_ORDER false)",
	                       partitioned_directory)));
	REQUIRE(!fs.DirectoryExists(partitioned_directory));

	auto existing_directory = TestCreatePath("copy_abort_existing");
	if (fs.DirectoryExists(existing_directory)) {
		fs.RemoveDirectory(existing_directory);
	}
	fs.CreateDirectory(existing_directory);
	REQUIRE_FAIL(connection.Query(
	    StringUtil::Format("COPY (SELECT CASE WHEN i=8192 THEN error('injected') ELSE repeat('x', 100) END AS v "
	                       "FROM range(16384) t(i)) TO '%s' "
	                       "(FORMAT CSV, FILE_SIZE_BYTES '16KB', PRESERVE_ORDER false)",
	                       existing_directory)));
	REQUIRE(fs.DirectoryExists(existing_directory));
	idx_t remaining_files = 0;
	fs.ListFiles(existing_directory, [&](const string &, bool) { remaining_files++; });
	REQUIRE(remaining_files == 0);
	fs.RemoveDirectory(existing_directory);

	auto move_target = TestCreatePath("copy_abort_move.csv");
	auto move_tmp = fs.JoinPath(StringUtil::GetFilePath(move_target), "tmp_" + StringUtil::GetFileName(move_target));
	fs.TryRemoveFile(move_tmp);
	if (fs.DirectoryExists(move_target)) {
		fs.RemoveDirectory(move_target);
	}
	fs.CreateDirectory(move_target);
	REQUIRE_FAIL(connection.Query(StringUtil::Format(
	    "COPY (SELECT i FROM range(10) t(i)) TO '%s' (FORMAT CSV, USE_TMP_FILE true)", move_target)));
	REQUIRE(!fs.FileExists(move_tmp));
	REQUIRE(fs.DirectoryExists(move_target));
	fs.RemoveDirectory(move_target);

	auto blob_move_target = TestCreatePath("copy_abort_move.blob");
	auto blob_move_tmp =
	    fs.JoinPath(StringUtil::GetFilePath(blob_move_target), "tmp_" + StringUtil::GetFileName(blob_move_target));
	fs.TryRemoveFile(blob_move_tmp);
	if (fs.DirectoryExists(blob_move_target)) {
		fs.RemoveDirectory(blob_move_target);
	}
	fs.CreateDirectory(blob_move_target);
	REQUIRE_FAIL(connection.Query(StringUtil::Format(
	    "COPY (SELECT 'payload'::BLOB) TO '%s' (FORMAT BLOB, USE_TMP_FILE true)", blob_move_target)));
	REQUIRE(!fs.FileExists(blob_move_tmp));
	REQUIRE(fs.DirectoryExists(blob_move_target));
	fs.RemoveDirectory(blob_move_target);

	auto success_path = TestCreatePath("copy_abort_success.csv");
	fs.TryRemoveFile(success_path);
	REQUIRE_NO_FAIL(
	    connection.Query(StringUtil::Format("COPY (SELECT i FROM range(10) t(i)) TO '%s' (FORMAT CSV)", success_path)));
	REQUIRE(fs.FileExists(success_path));
	fs.RemoveFile(success_path);
}
