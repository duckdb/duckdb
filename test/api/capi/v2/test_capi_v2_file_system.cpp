#include "test_capi_v2.hpp"

#include <algorithm>
#include <string>
#include <vector>

// ---------------------------------------------------------------------------
// V2 file system tests: borrow the engine's file system, open files through
// it, and read/write/seek them.
// ---------------------------------------------------------------------------

namespace test_capi_v2 {

namespace {

duckdb_v2_file_system_handle FsOf(duckdb_v2_connection_handle conn) {
	duckdb_v2_file_system_handle fs = nullptr;
	REQUIRE(duckdb_v2_connection_get_file_system(conn, &fs, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(fs != nullptr);
	return fs;
}

// Opens with nothing but flags, which is what most of these cases need.
duckdb_v2_file_handle FsOpen(duckdb_v2_file_system_handle fs, const std::string &path,
                             const std::vector<DUCKDB_V2_FILE_FLAG> &flags) {
	duckdb_v2_file_handle handle = nullptr;
	auto path_str = Convert(path);
	auto rc = duckdb_v2_file_system_open(fs, &path_str, flags.data(), flags.size(), nullptr, &handle, nullptr);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(handle != nullptr);
	return handle;
}

// The same, reporting the code rather than asserting success.
DUCKDB_V2_ERROR FsTryOpen(duckdb_v2_file_system_handle fs, const std::string &path,
                          const std::vector<DUCKDB_V2_FILE_FLAG> &flags, duckdb_v2_file_handle *out) {
	auto path_str = Convert(path);
	return duckdb_v2_file_system_open(fs, &path_str, flags.data(), flags.size(), nullptr, out, nullptr);
}

void FsWrite(duckdb_v2_file_handle handle, const std::string &data) {
	idx_t written = 0;
	REQUIRE(duckdb_v2_file_write(handle, data.data(), data.size(), &written, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(written == data.size());
}

// Reads the whole file from the current position.
std::string FsReadAll(duckdb_v2_file_handle handle, idx_t capacity) {
	std::vector<char> buffer(capacity, '\0');
	idx_t read = 0;
	REQUIRE(duckdb_v2_file_read(handle, buffer.data(), capacity, &read, nullptr) == DUCKDB_V2_ERROR_NONE);
	return std::string(buffer.data(), read);
}

idx_t FsSize(duckdb_v2_file_handle handle) {
	idx_t size = 0;
	REQUIRE(duckdb_v2_file_size(handle, &size, nullptr) == DUCKDB_V2_ERROR_NONE);
	return size;
}

idx_t FsTell(duckdb_v2_file_handle handle) {
	idx_t position = 0;
	REQUIRE(duckdb_v2_file_tell(handle, &position, nullptr) == DUCKDB_V2_ERROR_NONE);
	return position;
}

} // namespace

TEST_CASE("V2 file system: write, read back, and seek", "[capi_v2][file_system]") {
	EnvFixture fx;
	auto fs = FsOf(fx.conn);
	auto path = duckdb::TestCreatePath("v2_fs_roundtrip.bin");

	{
		auto handle = FsOpen(fs, path, {DUCKDB_V2_FILE_FLAG_WRITE, DUCKDB_V2_FILE_FLAG_CREATE_NEW});
		FsWrite(handle, "hello world");
		REQUIRE(FsTell(handle) == 11);
		REQUIRE(FsSize(handle) == 11);
		REQUIRE(duckdb_v2_file_sync(handle, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_file_destroy(&handle) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(handle == nullptr);
	}

	auto handle = FsOpen(fs, path, {DUCKDB_V2_FILE_FLAG_READ});
	REQUIRE(FsSize(handle) == 11);
	REQUIRE(FsReadAll(handle, 32) == "hello world");
	// Reading again at the end yields nothing, which is not an error.
	REQUIRE(FsReadAll(handle, 32).empty());

	// Seeking moves the position; a partial read stops where asked.
	REQUIRE(duckdb_v2_file_seek(handle, 6, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(FsTell(handle) == 6);
	REQUIRE(FsReadAll(handle, 5) == "world");

	REQUIRE(duckdb_v2_file_seek(handle, 0, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(FsReadAll(handle, 5) == "hello");

	duckdb_v2_file_destroy(&handle);
}

TEST_CASE("V2 file system: borrowed from an instance or a connection", "[capi_v2][file_system]") {
	EnvFixture fx;
	duckdb_v2_file_system_handle from_conn = nullptr;
	REQUIRE(duckdb_v2_connection_get_file_system(fx.conn, &from_conn, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(from_conn != nullptr);
	// Borrowed: there is no destroy, and asking twice gives the same file system.
	duckdb_v2_file_system_handle again = nullptr;
	REQUIRE(duckdb_v2_connection_get_file_system(fx.conn, &again, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(again == from_conn);

	// The instance has a file system of its own, which opens files just the same.
	duckdb_v2_file_system_handle from_instance = nullptr;
	REQUIRE(duckdb_v2_instance_get_file_system(fx.instance, &from_instance, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(from_instance != nullptr);
	REQUIRE(from_instance != from_conn);
	auto path = duckdb::TestCreatePath("v2_fs_instance.txt");
	auto handle = FsOpen(from_instance, path, {DUCKDB_V2_FILE_FLAG_WRITE, DUCKDB_V2_FILE_FLAG_CREATE_NEW});
	duckdb_v2_file_destroy(&handle);
}

TEST_CASE("V2 file system: CREATE opens an existing file as it is", "[capi_v2][file_system]") {
	EnvFixture fx;
	auto fs = FsOf(fx.conn);
	auto path = duckdb::TestCreatePath("v2_fs_create.bin");

	{
		auto handle = FsOpen(fs, path, {DUCKDB_V2_FILE_FLAG_WRITE, DUCKDB_V2_FILE_FLAG_CREATE_NEW});
		FsWrite(handle, "original");
		duckdb_v2_file_destroy(&handle);
	}
	{
		// CREATE on an existing file leaves the contents alone.
		auto handle = FsOpen(fs, path, {DUCKDB_V2_FILE_FLAG_WRITE, DUCKDB_V2_FILE_FLAG_CREATE});
		REQUIRE(FsSize(handle) == 8);
		duckdb_v2_file_destroy(&handle);
	}
}

TEST_CASE("V2 file system: CREATE_NEW truncates an existing file", "[capi_v2][file_system]") {
	EnvFixture fx;
	auto fs = FsOf(fx.conn);
	auto path = duckdb::TestCreatePath("v2_fs_create_new.bin");

	{
		auto handle = FsOpen(fs, path, {DUCKDB_V2_FILE_FLAG_WRITE, DUCKDB_V2_FILE_FLAG_CREATE});
		FsWrite(handle, "hello");
		duckdb_v2_file_destroy(&handle);
	}
	{
		// CREATE_NEW truncates rather than failing: the earlier contents are gone.
		auto handle = FsOpen(fs, path, {DUCKDB_V2_FILE_FLAG_WRITE, DUCKDB_V2_FILE_FLAG_CREATE_NEW});
		REQUIRE(FsSize(handle) == 0);
		FsWrite(handle, "hi");
		duckdb_v2_file_destroy(&handle);
	}
	{
		auto handle = FsOpen(fs, path, {DUCKDB_V2_FILE_FLAG_READ});
		REQUIRE(FsReadAll(handle, 32) == "hi");
		duckdb_v2_file_destroy(&handle);
	}
}

TEST_CASE("V2 file system: EXCLUSIVE_CREATE refuses an existing file", "[capi_v2][file_system]") {
	EnvFixture fx;
	auto fs = FsOf(fx.conn);
	auto path = duckdb::TestCreatePath("v2_fs_exclusive.bin");

	{
		auto handle = FsOpen(fs, path, {DUCKDB_V2_FILE_FLAG_WRITE, DUCKDB_V2_FILE_FLAG_CREATE});
		FsWrite(handle, "taken");
		duckdb_v2_file_destroy(&handle);
	}

	// The file is there, so the exclusive create fails rather than opening or truncating it.
	duckdb_v2_file_handle handle = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	(void)err;
	REQUIRE(FsTryOpen(fs, path,
	                  {DUCKDB_V2_FILE_FLAG_WRITE, DUCKDB_V2_FILE_FLAG_CREATE, DUCKDB_V2_FILE_FLAG_EXCLUSIVE_CREATE},
	                  &handle) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(handle == nullptr);

	// The contents survived.
	auto reader = FsOpen(fs, path, {DUCKDB_V2_FILE_FLAG_READ});
	REQUIRE(FsReadAll(reader, 32) == "taken");
	duckdb_v2_file_destroy(&reader);
}

TEST_CASE("V2 file system: APPEND writes at the end", "[capi_v2][file_system]") {
	EnvFixture fx;
	auto fs = FsOf(fx.conn);
	auto path = duckdb::TestCreatePath("v2_fs_append.bin");

	{
		auto handle = FsOpen(fs, path, {DUCKDB_V2_FILE_FLAG_WRITE, DUCKDB_V2_FILE_FLAG_CREATE_NEW});
		FsWrite(handle, "one");
		duckdb_v2_file_destroy(&handle);
	}
	{
		auto handle = FsOpen(fs, path, {DUCKDB_V2_FILE_FLAG_WRITE, DUCKDB_V2_FILE_FLAG_APPEND});
		FsWrite(handle, "two");
		duckdb_v2_file_destroy(&handle);
	}
	{
		auto handle = FsOpen(fs, path, {DUCKDB_V2_FILE_FLAG_READ});
		REQUIRE(FsReadAll(handle, 32) == "onetwo");
		duckdb_v2_file_destroy(&handle);
	}
}

TEST_CASE("V2 file system: open refusals", "[capi_v2][file_system]") {
	EnvFixture fx;
	auto fs = FsOf(fx.conn);
	duckdb_v2_file_handle handle = nullptr;

	// A missing file without CREATE.
	auto missing = duckdb::TestCreatePath("v2_fs_missing.bin");
	REQUIRE(FsTryOpen(fs, missing, {DUCKDB_V2_FILE_FLAG_READ}, &handle) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(handle == nullptr);

	// An open without flags cannot say whether the file is being read or written.
	auto path = duckdb::TestCreatePath("v2_fs_flags.bin");
	REQUIRE(FsTryOpen(fs, path, {}, &handle) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(handle == nullptr);
	// INVALID names no behaviour, and neither does a value outside the enum.
	REQUIRE(FsTryOpen(fs, path, {DUCKDB_V2_FILE_FLAG_INVALID}, &handle) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(FsTryOpen(fs, path, {static_cast<DUCKDB_V2_FILE_FLAG>(99)}, &handle) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(handle == nullptr);
}

TEST_CASE("V2 file system: close leaves the handle destroyable", "[capi_v2][file_system]") {
	EnvFixture fx;
	auto fs = FsOf(fx.conn);
	auto path = duckdb::TestCreatePath("v2_fs_close.bin");

	auto handle = FsOpen(fs, path, {DUCKDB_V2_FILE_FLAG_WRITE, DUCKDB_V2_FILE_FLAG_CREATE_NEW});
	FsWrite(handle, "data");
	REQUIRE(duckdb_v2_file_close(handle, nullptr) == DUCKDB_V2_ERROR_NONE);
	// The handle itself is still alive and must still be destroyed.
	REQUIRE(duckdb_v2_file_destroy(&handle) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(handle == nullptr);
}

TEST_CASE("V2 file system: null arguments and destroy null-safety", "[capi_v2][file_system]") {
	EnvFixture fx;
	auto fs = FsOf(fx.conn);
	auto path = duckdb::TestCreatePath("v2_fs_nulls.bin");
	auto handle = FsOpen(fs, path, {DUCKDB_V2_FILE_FLAG_WRITE, DUCKDB_V2_FILE_FLAG_CREATE_NEW});

	duckdb_v2_file_system_handle out_fs = nullptr;
	duckdb_v2_file_handle out_handle = nullptr;
	idx_t count = 0;
	char buffer[4] = {0};

	REQUIRE(duckdb_v2_context_get_file_system(nullptr, &out_fs, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_connection_get_file_system(nullptr, &out_fs, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_instance_get_file_system(nullptr, &out_fs, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_connection_get_file_system(fx.conn, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	const DUCKDB_V2_FILE_FLAG read_flag = DUCKDB_V2_FILE_FLAG_READ;
	auto path_str = Convert(path);
	REQUIRE(duckdb_v2_file_system_open(nullptr, &path_str, &read_flag, 1, nullptr, &out_handle, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_file_system_open(fs, nullptr, &read_flag, 1, nullptr, &out_handle, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_file_system_open(fs, &path_str, nullptr, 1, nullptr, &out_handle, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_file_system_open(fs, &path_str, &read_flag, 1, nullptr, nullptr, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);

	REQUIRE(duckdb_v2_file_read(nullptr, buffer, 4, &count, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_file_read(handle, nullptr, 4, &count, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_file_read(handle, buffer, 4, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_file_write(nullptr, buffer, 4, &count, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_file_write(handle, nullptr, 4, &count, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_file_tell(nullptr, &count, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_file_tell(handle, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_file_size(nullptr, &count, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_file_size(handle, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_file_seek(nullptr, 0, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_file_sync(nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_file_close(nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);

	REQUIRE(duckdb_v2_file_destroy(&handle) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(handle == nullptr);
	REQUIRE(duckdb_v2_file_destroy(&handle) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_file_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2 file system: positional read and write", "[capi_v2][file_system]") {
	EnvFixture fx;
	auto fs = FsOf(fx.conn);
	auto path = duckdb::TestCreatePath("v2_fs_positional.bin");
	const auto flags = {DUCKDB_V2_FILE_FLAG_READ, DUCKDB_V2_FILE_FLAG_WRITE, DUCKDB_V2_FILE_FLAG_CREATE_NEW,
	                    DUCKDB_V2_FILE_FLAG_PARALLEL_ACCESS};

	auto handle = FsOpen(fs, path, flags);
	const std::string data = "abcdefghij";
	FsWrite(handle, data);
	REQUIRE(FsTell(handle) == 10);

	// A positional read does not move the position.
	char buffer[4] = {0};
	idx_t bytes_read = 0;
	REQUIRE(duckdb_v2_file_read_at(handle, buffer, 4, 2, &bytes_read, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(bytes_read == 4);
	REQUIRE(std::string(buffer, 4) == "cdef");
	REQUIRE(FsTell(handle) == 10);

	// Neither does a positional write.
	idx_t bytes_written = 0;
	REQUIRE(duckdb_v2_file_write_at(handle, "XY", 2, 4, &bytes_written, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(bytes_written == 2);
	REQUIRE(FsTell(handle) == 10);
	REQUIRE(duckdb_v2_file_read_at(handle, buffer, 4, 2, &bytes_read, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(bytes_read == 4);
	REQUIRE(std::string(buffer, 4) == "cdXY");

	// A read crossing the end comes up short, and one at or past it reads nothing.
	REQUIRE(duckdb_v2_file_read_at(handle, buffer, 4, 8, &bytes_read, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(bytes_read == 2);
	REQUIRE(std::string(buffer, 2) == "ij");
	REQUIRE(duckdb_v2_file_read_at(handle, buffer, 4, 10, &bytes_read, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(bytes_read == 0);

	// A positional write past the end extends the file.
	REQUIRE(duckdb_v2_file_write_at(handle, "Z", 1, 15, &bytes_written, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(bytes_written == 1);
	REQUIRE(FsSize(handle) == 16);

	duckdb_v2_file_destroy(&handle);

	// The sequential view of the file agrees with what the positional writes did.
	auto reader = FsOpen(fs, path, {DUCKDB_V2_FILE_FLAG_READ});
	REQUIRE(FsReadAll(reader, 32).substr(0, 10) == "abcdXYghij");
	duckdb_v2_file_destroy(&reader);
}

TEST_CASE("V2 file system: truncate and abort", "[capi_v2][file_system]") {
	EnvFixture fx;
	auto fs = FsOf(fx.conn);

	auto path = duckdb::TestCreatePath("v2_fs_truncate.bin");
	auto handle = FsOpen(fs, path, {DUCKDB_V2_FILE_FLAG_WRITE, DUCKDB_V2_FILE_FLAG_CREATE_NEW});
	FsWrite(handle, "abcdefghij");
	REQUIRE(duckdb_v2_file_truncate(handle, 4, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(FsSize(handle) == 4);
	REQUIRE(duckdb_v2_file_truncate(nullptr, 4, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_file_destroy(&handle);

	// Aborting a file created exclusively removes it, and leaves the handle to be destroyed.
	auto aborted_path = duckdb::TestCreatePath("v2_fs_abort.bin");
	duckdb::FileSystem::CreateLocal()->TryRemoveFile(aborted_path);
	auto aborted =
	    FsOpen(fs, aborted_path,
	           {DUCKDB_V2_FILE_FLAG_WRITE, DUCKDB_V2_FILE_FLAG_CREATE, DUCKDB_V2_FILE_FLAG_EXCLUSIVE_CREATE});
	FsWrite(aborted, "partial");
	REQUIRE(duckdb_v2_file_abort(aborted, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_file_abort(nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_file_destroy(&aborted);
	REQUIRE(!duckdb::FileSystem::CreateLocal()->FileExists(aborted_path));
}

TEST_CASE("V2 file system: positional read and write null arguments", "[capi_v2][file_system]") {
	EnvFixture fx;
	auto fs = FsOf(fx.conn);
	auto path = duckdb::TestCreatePath("v2_fs_positional_nulls.bin");
	auto handle = FsOpen(fs, path, {DUCKDB_V2_FILE_FLAG_WRITE, DUCKDB_V2_FILE_FLAG_CREATE_NEW});
	char buffer[4] = {0};

	idx_t bytes_read = 0;
	REQUIRE(duckdb_v2_file_read_at(nullptr, buffer, 4, 0, &bytes_read, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_file_read_at(handle, nullptr, 4, 0, &bytes_read, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_file_read_at(handle, buffer, 4, 0, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	idx_t bytes_written = 0;
	REQUIRE(duckdb_v2_file_write_at(nullptr, buffer, 4, 0, &bytes_written, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_file_write_at(handle, nullptr, 4, 0, &bytes_written, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_file_write_at(handle, buffer, 4, 0, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);

	duckdb_v2_file_destroy(&handle);
}

TEST_CASE("V2 file system: metadata accompanies an open", "[capi_v2][file_system]") {
	EnvFixture fx;
	auto fs = FsOf(fx.conn);
	auto path = duckdb::TestCreatePath("v2_fs_options.bin");

	duckdb_v2_file_metadata_handle metadata = nullptr;
	REQUIRE(duckdb_v2_file_metadata_create(&metadata, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(metadata != nullptr);
	REQUIRE(duckdb_v2_file_metadata_set_size(metadata, 4, nullptr) == DUCKDB_V2_ERROR_NONE);

	// Values a file system does not recognise are carried and ignored rather than rejected.
	auto unknown = MakeVarcharValue(fx.factory, "nobody-reads-this");
	auto made_up_option_str = Convert("made_up_option");
	REQUIRE(duckdb_v2_file_metadata_set_value(metadata, &made_up_option_str, unknown, nullptr) == DUCKDB_V2_ERROR_NONE);
	// Copied at the call, so the value can go immediately.
	duckdb_v2_value_destroy(&unknown);
	duckdb_v2_value_handle read_back = nullptr;
	REQUIRE(duckdb_v2_file_metadata_get_value(metadata, &made_up_option_str, &read_back, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(read_back != nullptr);
	duckdb_v2_value_destroy(&read_back);
	auto another_option_str = Convert("another_option");
	REQUIRE(duckdb_v2_file_metadata_get_value(metadata, &another_option_str, &read_back, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(read_back == nullptr);

	// Passing the same flag twice is harmless.
	const std::vector<DUCKDB_V2_FILE_FLAG> flags {DUCKDB_V2_FILE_FLAG_WRITE, DUCKDB_V2_FILE_FLAG_CREATE_NEW,
	                                              DUCKDB_V2_FILE_FLAG_WRITE};
	duckdb_v2_file_handle handle = nullptr;
	auto path_str = Convert(path);
	REQUIRE(duckdb_v2_file_system_open(fs, &path_str, flags.data(), flags.size(), metadata, &handle, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	FsWrite(handle, "data");
	duckdb_v2_file_destroy(&handle);

	// The same metadata accompanies as many opens as you like, and is copied by another.
	auto second = duckdb::TestCreatePath("v2_fs_options_second.bin");
	auto second_str = Convert(second);
	REQUIRE(duckdb_v2_file_system_open(fs, &second_str, flags.data(), flags.size(), metadata, &handle, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	duckdb_v2_file_destroy(&handle);
	duckdb_v2_file_metadata_handle copy = nullptr;
	REQUIRE(duckdb_v2_file_metadata_create(&copy, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_file_metadata_copy(copy, metadata, nullptr) == DUCKDB_V2_ERROR_NONE);
	idx_t size = 0;
	bool known = false;
	REQUIRE(duckdb_v2_file_metadata_get_size(copy, &size, &known, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(known);
	REQUIRE(size == 4);
	REQUIRE(duckdb_v2_file_metadata_get_value(copy, &made_up_option_str, &read_back, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(read_back != nullptr);
	duckdb_v2_value_destroy(&read_back);
	duckdb_v2_file_metadata_destroy(&copy);

	REQUIRE(duckdb_v2_file_metadata_destroy(&metadata) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(metadata == nullptr);

	auto reader = FsOpen(fs, path, {DUCKDB_V2_FILE_FLAG_READ});
	REQUIRE(FsReadAll(reader, 32) == "data");
	duckdb_v2_file_destroy(&reader);
}

TEST_CASE("V2 file system: metadata value null arguments and reserved names", "[capi_v2][file_system]") {
	EnvFixture fx;
	duckdb_v2_file_metadata_handle metadata = nullptr;
	REQUIRE(duckdb_v2_file_metadata_create(&metadata, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto value = MakeInt64Value(fx.factory, 1);
	duckdb_v2_value_handle out = nullptr;

	REQUIRE(duckdb_v2_file_metadata_create(nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	auto k_str = Convert("k");
	REQUIRE(duckdb_v2_file_metadata_set_value(nullptr, &k_str, value, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_file_metadata_set_value(metadata, &k_str, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_file_metadata_get_value(nullptr, &k_str, &out, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_file_metadata_get_value(metadata, &k_str, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_file_metadata_copy(nullptr, metadata, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_file_metadata_copy(metadata, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	// An empty name is not a usable key, and the typed fields have setters of their own.
	auto empty_str = Convert("");
	REQUIRE(duckdb_v2_file_metadata_set_value(metadata, &empty_str, value, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	auto file_size_str = Convert("file_size");
	REQUIRE(duckdb_v2_file_metadata_set_value(metadata, &file_size_str, value, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	auto file_size_str2 = Convert("FILE_SIZE");
	REQUIRE(duckdb_v2_file_metadata_set_value(metadata, &file_size_str2, value, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);

	// Names are case-sensitive, and values are kept as given: whoever reads one casts it.
	auto flag = MakeVarcharValue(fx.factory, "sometimes");
	auto force_full_download_str = Convert("FORCE_FULL_DOWNLOAD");
	REQUIRE(duckdb_v2_file_metadata_set_value(metadata, &force_full_download_str, flag, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	duckdb_v2_value_destroy(&flag);
	REQUIRE(duckdb_v2_file_metadata_get_value(metadata, &force_full_download_str, &out, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(out != nullptr);
	duckdb_v2_value_destroy(&out);
	auto force_full_download_str2 = Convert("force_full_download");
	REQUIRE(duckdb_v2_file_metadata_get_value(metadata, &force_full_download_str2, &out, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(out == nullptr);

	duckdb_v2_value_destroy(&value);
	duckdb_v2_file_metadata_destroy(&metadata);
}

TEST_CASE("V2 file system: path queries", "[capi_v2][file_system]") {
	EnvFixture fx;
	auto fs = FsOf(fx.conn);

	auto exists = [&](const std::string &path) {
		auto path_str = Convert(path);
		bool result = false;
		REQUIRE(duckdb_v2_file_system_path_exists(fs, &path_str, &result, nullptr) == DUCKDB_V2_ERROR_NONE);
		return result;
	};
	auto is_remote = [&](const std::string &path) {
		auto path_str = Convert(path);
		bool result = false;
		REQUIRE(duckdb_v2_file_system_path_is_remote(fs, &path_str, &result, nullptr) == DUCKDB_V2_ERROR_NONE);
		return result;
	};
	auto expand = [&](const std::string &path) {
		auto path_str = Convert(path);
		TextSinkTarget result;
		REQUIRE(duckdb_v2_file_system_path_expand(fs, &path_str, AppendToString, &result, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		REQUIRE(result.calls == 1);
		return result.text;
	};

	auto path = duckdb::TestCreatePath("v2_fs_path_exists.bin");
	REQUIRE_FALSE(exists(path));
	auto handle = FsOpen(fs, path, {DUCKDB_V2_FILE_FLAG_WRITE, DUCKDB_V2_FILE_FLAG_CREATE_NEW});
	duckdb_v2_file_destroy(&handle);
	REQUIRE(exists(path));
	// A directory is not a file.
	REQUIRE_FALSE(exists(duckdb::TestDirectoryPath()));

	REQUIRE(is_remote("s3://bucket/key.xlsx"));
	REQUIRE(is_remote("https://example.com/file.xlsx"));
	REQUIRE_FALSE(is_remote(path));
	REQUIRE_FALSE(is_remote("relative/file.xlsx"));

	// A leading ~ becomes the configured home directory; anything else is handed back as it is.
	duckdb_v2_result_handle result = nullptr;
	REQUIRE(Query(fx.conn, "SET home_directory = '/v2_home'", &result) == DUCKDB_V2_ERROR_NONE);
	DrainRowCount(result);
	duckdb_v2_result_destroy(&result);
	REQUIRE(expand("~/book.xlsx") == "/v2_home/book.xlsx");
	REQUIRE(expand("/abs/book.xlsx") == "/abs/book.xlsx");

	auto path_str = Convert(path);
	bool out = false;
	REQUIRE(duckdb_v2_file_system_path_exists(nullptr, &path_str, &out, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_file_system_path_is_remote(fs, &path_str, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_file_system_path_expand(fs, &path_str, nullptr, nullptr, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
}

namespace {

void FsCollectGlob(const duckdb_v2_str *path, void *user_data, duckdb_v2_error_info_handle *) {
	static_cast<std::vector<std::string> *>(user_data)->emplace_back(path->ptr, path->len);
}

void FsFailGlob(const duckdb_v2_str *, void *, duckdb_v2_error_info_handle *err) {
	auto code = DUCKDB_V2_ERROR_IO_GENERAL;
	duckdb_v2_error_info_set_code(*err, code);
}

std::vector<std::string> FsGlob(duckdb_v2_file_system_handle fs, const std::string &pattern) {
	std::vector<std::string> matches;
	auto pattern_str = Convert(pattern);
	REQUIRE(duckdb_v2_file_system_path_glob(fs, &pattern_str, FsCollectGlob, &matches, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	std::sort(matches.begin(), matches.end());
	return matches;
}

} // namespace

TEST_CASE("V2 file system: glob, move and remove", "[capi_v2][file_system]") {
	EnvFixture fx;
	auto fs = FsOf(fx.conn);
	auto first = duckdb::TestCreatePath("v2_fs_glob_a.globtest");
	auto second = duckdb::TestCreatePath("v2_fs_glob_b.globtest");
	for (auto &path : {first, second}) {
		auto handle = FsOpen(fs, path, {DUCKDB_V2_FILE_FLAG_WRITE, DUCKDB_V2_FILE_FLAG_CREATE_NEW});
		FsWrite(handle, "x");
		duckdb_v2_file_destroy(&handle);
	}

	auto pattern = duckdb::TestCreatePath("v2_fs_glob_*.globtest");
	auto matches = FsGlob(fs, pattern);
	REQUIRE(matches.size() == 2);
	REQUIRE(duckdb::StringUtil::EndsWith(matches[0], "v2_fs_glob_a.globtest"));
	REQUIRE(duckdb::StringUtil::EndsWith(matches[1], "v2_fs_glob_b.globtest"));
	REQUIRE(FsGlob(fs, duckdb::TestCreatePath("v2_fs_glob_none_*.globtest")).empty());
	// A callback error stops the glob.
	auto pattern_str = Convert(pattern);
	REQUIRE(duckdb_v2_file_system_path_glob(fs, &pattern_str, FsFailGlob, nullptr, nullptr) ==
	        DUCKDB_V2_ERROR_IO_GENERAL);

	auto moved = duckdb::TestCreatePath("v2_fs_glob_moved.globtest");
	auto first_str = Convert(first);
	auto moved_str = Convert(moved);
	REQUIRE(duckdb_v2_file_system_path_move(fs, &first_str, &moved_str, nullptr) == DUCKDB_V2_ERROR_NONE);
	bool exists = true;
	REQUIRE(duckdb_v2_file_system_path_exists(fs, &first_str, &exists, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE_FALSE(exists);
	REQUIRE(duckdb_v2_file_system_path_exists(fs, &moved_str, &exists, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(exists);
	REQUIRE(duckdb_v2_file_system_path_move(fs, &first_str, &moved_str, nullptr) != DUCKDB_V2_ERROR_NONE);

	bool removed = false;
	REQUIRE(duckdb_v2_file_system_path_remove(fs, &moved_str, &removed, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(removed);
	// Nothing to remove is not an error.
	REQUIRE(duckdb_v2_file_system_path_remove(fs, &moved_str, &removed, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE_FALSE(removed);
	auto second_str = Convert(second);
	REQUIRE(duckdb_v2_file_system_path_remove(fs, &second_str, nullptr, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(FsGlob(fs, pattern).empty());

	// Null arguments.
	REQUIRE(duckdb_v2_file_system_path_glob(fs, nullptr, FsCollectGlob, nullptr, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_file_system_path_glob(fs, &pattern_str, nullptr, nullptr, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_file_system_path_move(fs, nullptr, &moved_str, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_file_system_path_remove(nullptr, &moved_str, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
}

} // namespace test_capi_v2
