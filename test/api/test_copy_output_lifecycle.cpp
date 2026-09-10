#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/operator/persistent/copy_output_lifecycle.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/extension_manager.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

namespace {

struct LifecycleCopyInfo : CopyFunctionInfo {
	idx_t fail_finalize_after = DConstants::INVALID_INDEX;
	atomic<idx_t> finalize_attempts {0};
	mutex lock;
	vector<string> finalized_paths;
};

struct LifecycleCopyBindData : FunctionData {
	explicit LifecycleCopyBindData(shared_ptr<CopyFunctionInfo> info_p) : info(std::move(info_p)) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<LifecycleCopyBindData>(info);
	}

	bool Equals(const FunctionData &other_p) const override {
		return info == other_p.Cast<LifecycleCopyBindData>().info;
	}

	LifecycleCopyInfo &GetInfo() {
		return info->Cast<LifecycleCopyInfo>();
	}

	shared_ptr<CopyFunctionInfo> info;
};

struct LifecycleCopyLocalData : LocalFunctionData {};

struct LifecycleCopyPreparedData : PreparedBatchData {};

struct LifecycleCopyGlobalData : GlobalFunctionData {
	LifecycleCopyGlobalData(ClientContext &context, string path_p)
	    : path(std::move(path_p)),
	      handle(FileSystem::GetFileSystem(context).OpenFile(path, FileFlags::FILE_FLAGS_WRITE |
	                                                                   FileFlags::FILE_FLAGS_FILE_CREATE_NEW |
	                                                                   FileFlags::FILE_FLAGS_EXCLUSIVE_CREATE)) {
	}

	~LifecycleCopyGlobalData() override {
		if (!handle) {
			return;
		}
		try {
			handle->AbortWrite();
		} catch (...) { // NOLINT
		}
	}

	string path;
	unique_ptr<FileHandle> handle;
	idx_t flushed_batches = 0;
};

unique_ptr<FunctionData> LifecycleCopyBind(ClientContext &, CopyFunctionBindInput &input, const vector<Identifier> &,
                                           const vector<LogicalType> &) {
	return make_uniq<LifecycleCopyBindData>(input.function_info);
}

unique_ptr<LocalFunctionData> LifecycleCopyInitializeLocal(ExecutionContext &, FunctionData &) {
	return make_uniq<LifecycleCopyLocalData>();
}

unique_ptr<GlobalFunctionData> LifecycleCopyInitializeGlobal(ClientContext &context, FunctionData &,
                                                             const string &path) {
	return make_uniq<LifecycleCopyGlobalData>(context, path);
}

void LifecycleCopySink(ExecutionContext &, FunctionData &, GlobalFunctionData &, LocalFunctionData &, DataChunk &) {
}

void LifecycleCopyCombine(ExecutionContext &, FunctionData &, GlobalFunctionData &, LocalFunctionData &) {
}

void LifecycleCopyFinalize(ClientContext &, FunctionData &bind_data, GlobalFunctionData &global_data) {
	auto &info = bind_data.Cast<LifecycleCopyBindData>().GetInfo();
	auto attempt = info.finalize_attempts.fetch_add(1);
	if (attempt >= info.fail_finalize_after) {
		throw IOException("Injected COPY finalization failure");
	}
	auto &state = global_data.Cast<LifecycleCopyGlobalData>();
	state.handle->Close();
	state.handle.reset();
	lock_guard<mutex> guard(info.lock);
	info.finalized_paths.push_back(state.path);
}

void LifecycleCopyGetStatistics(ClientContext &, FunctionData &, GlobalFunctionData &,
                                CopyFunctionFileStatistics &statistics) {
	statistics.footer_size_bytes = Value();
}

CopyFunctionExecutionMode LifecycleRegularExecutionMode(bool, bool) {
	return CopyFunctionExecutionMode::REGULAR_COPY_TO_FILE;
}

CopyFunctionExecutionMode LifecycleBatchExecutionMode(bool, bool) {
	return CopyFunctionExecutionMode::BATCH_COPY_TO_FILE;
}

unique_ptr<PreparedBatchData> LifecycleCopyPrepareBatch(ClientContext &, FunctionData &, GlobalFunctionData &,
                                                        unique_ptr<ColumnDataCollection>) {
	return make_uniq<LifecycleCopyPreparedData>();
}

void LifecycleCopyFlushBatch(ClientContext &, FunctionData &, GlobalFunctionData &global_data, PreparedBatchData &) {
	global_data.Cast<LifecycleCopyGlobalData>().flushed_batches++;
}

idx_t LifecycleCopyFileSize(GlobalFunctionData &global_data) {
	return global_data.Cast<LifecycleCopyGlobalData>().flushed_batches;
}

CopyFunction CreateLifecycleCopyFunction(const string &name, const shared_ptr<LifecycleCopyInfo> &info,
                                         bool batch_mode = false) {
	CopyFunction function {Identifier(name)};
	function.copy_to_bind = LifecycleCopyBind;
	function.copy_to_initialize_local = LifecycleCopyInitializeLocal;
	function.copy_to_initialize_global = LifecycleCopyInitializeGlobal;
	function.copy_to_get_written_statistics = LifecycleCopyGetStatistics;
	function.copy_to_sink = LifecycleCopySink;
	function.copy_to_combine = LifecycleCopyCombine;
	function.copy_to_finalize = LifecycleCopyFinalize;
	function.execution_mode = batch_mode ? LifecycleBatchExecutionMode : LifecycleRegularExecutionMode;
	function.prepare_batch = LifecycleCopyPrepareBatch;
	function.flush_batch = LifecycleCopyFlushBatch;
	function.file_size_bytes = LifecycleCopyFileSize;
	function.extension = "test";
	function.function_info = info;
	return function;
}

void RegisterLifecycleCopyFunction(DuckDB &db, const string &name, const shared_ptr<LifecycleCopyInfo> &info,
                                   bool batch_mode = false) {
	ExtensionInfo extension_info {};
	ExtensionActiveLoad load_info {*db.instance, extension_info, "copy_output_lifecycle_test", ""};
	ExtensionLoader loader {load_info};
	loader.RegisterFunction(CreateLifecycleCopyFunction(name, info, batch_mode));
}

void RemoveDirectoryIfPresent(FileSystem &fs, const string &path) {
	if (fs.DirectoryExists(path)) {
		fs.RemoveDirectoryExtended(path, {RemoveDirectoryMode::RECURSIVE});
	}
}

} // namespace

TEST_CASE("COPY output lifecycle removes only finalized owned files", "[api][copy]") {
	DuckDB db(nullptr);
	Connection connection(db);
	auto &fs = FileSystem::GetFileSystem(*connection.context);

	auto finalized_path = TestCreatePath("copy_lifecycle_finalized.test");
	fs.TryRemoveFile(finalized_path);
	{
		CopyOutputLifecycle lifecycle(*connection.context);
		auto file_index = lifecycle.RegisterFile(finalized_path);
		auto handle = fs.OpenFile(finalized_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
		handle->Close();
		lifecycle.MarkFileFinalized(file_index);
	}
	REQUIRE(!fs.FileExists(finalized_path));

	auto incomplete_path = TestCreatePath("copy_lifecycle_incomplete.test");
	fs.TryRemoveFile(incomplete_path);
	{
		CopyOutputLifecycle lifecycle(*connection.context);
		lifecycle.RegisterFile(incomplete_path);
		auto handle = fs.OpenFile(incomplete_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
		handle->Close();
	}
	REQUIRE(fs.FileExists(incomplete_path));
	fs.RemoveFile(incomplete_path);

	auto existing_path = TestCreatePath("copy_lifecycle_existing.test");
	fs.TryRemoveFile(existing_path);
	{
		auto handle = fs.OpenFile(existing_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
		handle->Close();
	}
	{
		CopyOutputLifecycle lifecycle(*connection.context);
		auto file_index = lifecycle.RegisterFile(existing_path);
		lifecycle.MarkFileFinalized(file_index);
	}
	REQUIRE(fs.FileExists(existing_path));
	fs.RemoveFile(existing_path);

	auto successful_path = TestCreatePath("copy_lifecycle_success.test");
	fs.TryRemoveFile(successful_path);
	{
		CopyOutputLifecycle lifecycle(*connection.context);
		auto file_index = lifecycle.RegisterFile(successful_path);
		auto handle = fs.OpenFile(successful_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
		handle->Close();
		lifecycle.MarkFileFinalized(file_index);
		lifecycle.MarkSuccessful();
	}
	REQUIRE(fs.FileExists(successful_path));
	fs.RemoveFile(successful_path);
}

TEST_CASE("COPY output lifecycle removes only empty query-created directories", "[api][copy]") {
	DuckDB db(nullptr);
	Connection connection(db);
	auto &fs = FileSystem::GetFileSystem(*connection.context);

	auto root = TestCreatePath("copy_lifecycle_directories");
	RemoveDirectoryIfPresent(fs, root);
	auto child = fs.JoinPath(root, "child");
	fs.CreateDirectoryExtended(child, {CreateDirectoryMode::RECURSIVE});
	{
		CopyOutputLifecycle lifecycle(*connection.context);
		lifecycle.RegisterCreatedDirectory(root);
		lifecycle.RegisterCreatedDirectory(child);
	}
	REQUIRE(!fs.DirectoryExists(child));
	REQUIRE(!fs.DirectoryExists(root));

	fs.CreateDirectory(root);
	fs.CreateDirectory(child);
	auto retained_file = fs.JoinPath(child, "retained.test");
	{
		auto handle = fs.OpenFile(retained_file, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
		handle->Close();
	}
	{
		CopyOutputLifecycle lifecycle(*connection.context);
		lifecycle.RegisterCreatedDirectory(root);
		lifecycle.RegisterCreatedDirectory(child);
	}
	REQUIRE(fs.FileExists(retained_file));
	REQUIRE(fs.DirectoryExists(child));
	REQUIRE(fs.DirectoryExists(root));
	RemoveDirectoryIfPresent(fs, root);
}

TEST_CASE("COPY removes finalized partition outputs after a later finalization failure", "[api][copy]") {
	DuckDB db(nullptr);
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("SET threads=1"));
	REQUIRE_NO_FAIL(connection.Query("SET async_threads=0"));
	auto &fs = FileSystem::GetFileSystem(*connection.context);

	auto info = make_shared_ptr<LifecycleCopyInfo>();
	info->fail_finalize_after = 1;
	RegisterLifecycleCopyFunction(db, "copy_lifecycle_partition_failure", info);

	auto output = TestCreatePath("copy_lifecycle_partition_failure");
	RemoveDirectoryIfPresent(fs, output);
	auto result = connection.Query(StringUtil::Format("COPY (SELECT i AS p, 42 AS v FROM range(4) t(i)) TO '%s' "
	                                                  "(FORMAT copy_lifecycle_partition_failure, PARTITION_BY (p))",
	                                                  output));
	REQUIRE_FAIL(result);
	result.reset();
	REQUIRE(info->finalize_attempts > 1);
	REQUIRE(!info->finalized_paths.empty());
	for (auto &path : info->finalized_paths) {
		REQUIRE(!fs.FileExists(path));
	}
	REQUIRE(!fs.DirectoryExists(output));
}

TEST_CASE("COPY removes rotated outputs after a later finalization failure", "[api][copy]") {
	DuckDB db(nullptr);
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("SET threads=1"));
	REQUIRE_NO_FAIL(connection.Query("SET async_threads=0"));
	auto &fs = FileSystem::GetFileSystem(*connection.context);

	auto info = make_shared_ptr<LifecycleCopyInfo>();
	info->fail_finalize_after = 1;
	RegisterLifecycleCopyFunction(db, "copy_lifecycle_rotation_failure", info);
	auto output = TestCreatePath("copy_lifecycle_rotation_failure");
	RemoveDirectoryIfPresent(fs, output);
	auto result = connection.Query(StringUtil::Format(
	    "COPY (SELECT i FROM range(8192) t(i)) TO '%s' "
	    "(FORMAT copy_lifecycle_rotation_failure, FILE_SIZE_BYTES '1B', BATCH_SIZE 2048, PRESERVE_ORDER false)",
	    output));
	REQUIRE_FAIL(result);
	result.reset();
	REQUIRE(info->finalize_attempts > 1);
	REQUIRE(!info->finalized_paths.empty());
	for (auto &path : info->finalized_paths) {
		REQUIRE(!fs.FileExists(path));
	}
	REQUIRE(!fs.DirectoryExists(output));
}

TEST_CASE("COPY cleans a finalized temporary file when its move fails", "[api][copy]") {
	for (auto batch_mode : {false, true}) {
		DuckDB db(nullptr);
		Connection connection(db);
		REQUIRE_NO_FAIL(connection.Query(batch_mode ? "SET threads=4" : "SET threads=1"));
		if (batch_mode) {
			REQUIRE_NO_FAIL(connection.Query("CREATE TABLE copy_lifecycle_batch_input AS FROM range(4096)"));
		}
		auto &fs = FileSystem::GetFileSystem(*connection.context);
		string name = batch_mode ? "copy_lifecycle_batch_move" : "copy_lifecycle_regular_move";
		auto info = make_shared_ptr<LifecycleCopyInfo>();
		RegisterLifecycleCopyFunction(db, name, info, batch_mode);

		auto target = TestCreatePath(name + ".test");
		auto temporary = fs.JoinPath(StringUtil::GetFilePath(target), "tmp_" + StringUtil::GetFileName(target));
		fs.TryRemoveFile(temporary);
		RemoveDirectoryIfPresent(fs, target);
		fs.CreateDirectory(target);
		auto source = batch_mode ? "copy_lifecycle_batch_input" : "(SELECT i FROM range(4096) t(i))";
		auto result = connection.Query(StringUtil::Format(
		    "COPY %s TO '%s' (FORMAT %s, USE_TMP_FILE true, PRESERVE_ORDER false)", source, target, name));
		REQUIRE_FAIL(result);
		result.reset();
		REQUIRE(!fs.FileExists(temporary));
		REQUIRE(fs.DirectoryExists(target));
		fs.RemoveDirectory(target);
	}
}

TEST_CASE("Abandoning multi-chunk COPY statistics preserves finalized files", "[api][copy]") {
	DuckDB db(nullptr);
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("SET threads=1"));
	REQUIRE_NO_FAIL(connection.Query("SET async_threads=0"));
	auto &fs = FileSystem::GetFileSystem(*connection.context);

	auto info = make_shared_ptr<LifecycleCopyInfo>();
	RegisterLifecycleCopyFunction(db, "copy_lifecycle_abandon_stats", info);
	auto output = TestCreatePath("copy_lifecycle_abandon_stats");
	RemoveDirectoryIfPresent(fs, output);

	auto result =
	    connection.SendQuery(StringUtil::Format("COPY (SELECT i AS p, 42 AS v FROM range(%d) t(i)) TO '%s' "
	                                            "(FORMAT copy_lifecycle_abandon_stats, PARTITION_BY (p), RETURN_STATS)",
	                                            STANDARD_VECTOR_SIZE + 1, output));
	REQUIRE_NO_FAIL(*result);
	auto first_chunk = result->Fetch();
	REQUIRE(first_chunk);
	REQUIRE(first_chunk->size() == STANDARD_VECTOR_SIZE);
	result.reset();

	REQUIRE(info->finalized_paths.size() == STANDARD_VECTOR_SIZE + 1);
	for (auto &path : info->finalized_paths) {
		REQUIRE(fs.FileExists(path));
	}
	RemoveDirectoryIfPresent(fs, output);
}
