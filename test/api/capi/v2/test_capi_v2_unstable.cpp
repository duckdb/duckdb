// Tests of the unstable part of the V2 C API. Built as a translation unit of its own: the other V2 tests are built
// without the unstable surface, and verify that it stays off unless opted into.
#define DUCKDB_V2_API_ALLOW_UNSTABLE 1

#include "test_capi_v2.hpp"

#include "duckdb/common/local_file_system.hpp"
#include "duckdb/main/capi_v2/capi_v2_internal.hpp"

#include <cstring>
#include <fstream>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace test_capi_v2 {

namespace {

duckdb_v2_identifier_t UnstableIdent(const char *s) {
	return duckdb_v2_identifier_t {s, std::strlen(s)};
}

// Run a query producing a single BIGINT cell.
int64_t UnstableQueryI64(duckdb_v2_connection_handle conn, const char *sql) {
	duckdb_v2_result_handle result = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	auto rc = Query(conn, sql, &result, &err);
	std::string message;
	if (err) {
		duckdb_v2_str text = {nullptr, 0};
		duckdb_v2_error_info_get_text(err, &text);
		message = Convert(text);
		duckdb_v2_error_info_destroy(&err);
	}
	INFO(message);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
	auto chunk = StepChunk(result);
	REQUIRE(chunk != nullptr);
	duckdb_v2_vector_handle vec = nullptr;
	duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr);
	duckdb_v2_vector_view view {};
	duckdb_v2_vector_get_view(vec, &view, nullptr);
	auto out = static_cast<const int64_t *>(view.data)[SelAt(view.sel, 0)];
	duckdb_v2_data_chunk_destroy(&chunk);
	duckdb_v2_result_destroy(&result);
	return out;
}

} // namespace

// ---------------------------------------------------------------------------
// Open options of the file a multi-file function reads. open_probe_file(path) opens its file with the open options
// of its bind and produces no rows; open_probe wraps it. Paths under capture-open:// are served by a file system
// that reports a size for every file it globs - like a remote store listing a bucket - and records the options
// each file is opened with.
// ---------------------------------------------------------------------------

namespace {

std::mutex captured_open_lock;
std::vector<std::unordered_map<std::string, std::string>> captured_open_options;

class CaptureOpenFileSystem : public duckdb::LocalFileSystem {
public:
	explicit CaptureOpenFileSystem(std::string target_p) : target(std::move(target_p)) {
	}

	static constexpr const char *PREFIX = "capture-open://";

	std::string GetName() const override {
		return "CaptureOpenFileSystem";
	}
	bool CanHandleFile(const std::string &path) override {
		return duckdb::StringUtil::StartsWith(path, PREFIX);
	}
	duckdb::vector<duckdb::OpenFileInfo> Glob(const std::string &path, duckdb::FileOpener *) override {
		duckdb::OpenFileInfo info(path);
		info.extended_info = duckdb::make_shared_ptr<duckdb::ExtendedOpenFileInfo>();
		info.extended_info->options["file_size"] = duckdb::Value::UBIGINT(42);
		return {info};
	}
	bool SupportsGlobExtended() const override {
		return false;
	}
	bool SupportsOpenFileExtended() const override {
		return true;
	}

protected:
	duckdb::unique_ptr<duckdb::FileHandle> OpenFileExtended(const duckdb::OpenFileInfo &file,
	                                                        duckdb::FileOpenFlags flags,
	                                                        duckdb::optional_ptr<duckdb::FileOpener> opener) override {
		std::unordered_map<std::string, std::string> options;
		if (file.extended_info) {
			for (auto &entry : file.extended_info->options) {
				options[entry.first] = entry.second.ToString();
			}
		}
		{
			std::lock_guard<std::mutex> guard(captured_open_lock);
			captured_open_options.push_back(std::move(options));
		}
		return duckdb::LocalFileSystem::OpenFile(target, flags, opener);
	}

private:
	std::string target;
};

void OpenProbeBindCb(duckdb_v2_function_bind_info_handle info, duckdb_v2_table_function_bind_info_handle result,
                     duckdb_v2_context_handle context, duckdb_v2_error_info_handle *err) {
	duckdb_v2_value_handle path_value = nullptr;
	if (duckdb_v2_function_bind_get_arg_value(info, 0, &path_value, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	duckdb_v2_str path = {nullptr, 0};
	duckdb_v2_file_system_handle fs = nullptr;
	duckdb_v2_file_open_options_handle options = nullptr;
	duckdb_v2_file_handle file = nullptr;
	// the path is borrowed from its value, which outlives the open
	auto opened =
	    duckdb_v2_value_get_varchar(path_value, &path, err) == DUCKDB_V2_ERROR_NONE &&
	    duckdb_v2_file_system_get_from_context(context, &fs, err) == DUCKDB_V2_ERROR_NONE &&
	    duckdb_v2_table_function_bind_get_file_open_options(result, &options, err) == DUCKDB_V2_ERROR_NONE &&
	    duckdb_v2_file_open_options_set_flag(options, DUCKDB_V2_FILE_FLAG_READ, err) == DUCKDB_V2_ERROR_NONE &&
	    duckdb_v2_file_system_open(fs, &path, options, &file, err) == DUCKDB_V2_ERROR_NONE;
	duckdb_v2_file_destroy(&file);
	duckdb_v2_file_open_options_destroy(&options);
	duckdb_v2_value_destroy(&path_value);
	if (!opened) {
		return;
	}

	duckdb_v2_logical_type_handle bigint = nullptr;
	if (duckdb_v2_context_create_type_from_id(context, DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT, nullptr, nullptr, 0, &bigint,
	                                          err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	auto name_str = UnstableIdent("i");
	duckdb_v2_table_function_bind_add_result_column(result, &name_str, bigint, err);
	duckdb_v2_logical_type_destroy(&bigint);
}

void OpenProbeExecCb(duckdb_v2_table_function_exec_info_handle info, duckdb_v2_context_handle,
                     duckdb_v2_error_info_handle *err) {
	duckdb_v2_data_chunk_handle chunk = nullptr;
	duckdb_v2_vector_handle vec = nullptr;
	if (duckdb_v2_table_function_exec_get_output_chunk(info, &chunk, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	duckdb_v2_vector_set_size(vec, 0, err);
}

void RegisterOpenProbe(duckdb_v2_connection_handle conn) {
	auto varchar = MakeType(conn, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR);
	duckdb_v2_table_function_handle function = nullptr;
	REQUIRE(duckdb_v2_table_function_create_with_connection(conn, &function, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto function_name = Convert("open_probe_file");
	REQUIRE(duckdb_v2_table_function_set_name(function, &function_name, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_function_signature_handle sig = nullptr;
	REQUIRE(duckdb_v2_table_function_get_signature(function, &sig, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto param_name = UnstableIdent("path");
	REQUIRE(duckdb_v2_function_signature_add_parameter(sig, &param_name, varchar, nullptr,
	                                                   DUCKDB_V2_FUNCTION_PARAMETER_KIND_STANDARD,
	                                                   nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_set_bind_callback(function, OpenProbeBindCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_set_exec_callback(function, OpenProbeExecCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	// a multi-file function adds virtual columns (e.g. "filename") to the scan, which requires projection pushdown
	REQUIRE(duckdb_v2_table_function_set_projection_pushdown(function, true, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_register(function, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_table_function_destroy(&function);
	duckdb_v2_logical_type_destroy(&varchar);

	duckdb_v2_multi_file_function_handle multi_file = nullptr;
	REQUIRE(duckdb_v2_multi_file_function_create_with_connection(conn, &multi_file, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto name = Convert("open_probe");
	auto single_file = Convert("open_probe_file");
	REQUIRE(duckdb_v2_multi_file_function_set_name(multi_file, &name, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_multi_file_function_set_single_file_function(multi_file, &single_file, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_multi_file_function_register(multi_file, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_multi_file_function_destroy(&multi_file);
}

} // namespace

TEST_CASE("V2 table: the bind opens its file with the open options of the multi-file reader",
          "[capi_v2][table_function]") {
	EnvFixture fx;
	auto target = duckdb::TestCreatePath("capture_open_target");
	{ std::ofstream out(target); }
	auto &connection = *duckdb::capiv2::Convert(fx.conn);
	duckdb::FileSystem::GetFileSystem(*connection.context)
	    .RegisterSubSystem(duckdb::make_uniq<CaptureOpenFileSystem>(target));
	RegisterOpenProbe(fx.conn);

	// the size the file system reported while globbing reaches the open of the file
	captured_open_options.clear();
	REQUIRE(UnstableQueryI64(fx.conn, "SELECT count(*) FROM open_probe('capture-open://data.bin')") == 0);
	REQUIRE(captured_open_options.size() == 1);
	REQUIRE(captured_open_options[0]["file_size"] == "42");

	// called directly, the function reads a path and knows nothing more about it
	captured_open_options.clear();
	REQUIRE(UnstableQueryI64(fx.conn, "SELECT count(*) FROM open_probe_file('capture-open://data.bin')") == 0);
	REQUIRE(captured_open_options.size() == 1);
	REQUIRE(captured_open_options[0].empty());

	// null arguments
	duckdb_v2_file_open_options_handle options = nullptr;
	REQUIRE(duckdb_v2_table_function_bind_get_file_open_options(nullptr, &options, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(options == nullptr);
}

} // namespace test_capi_v2
