#include "capi_tester.hpp"

using namespace duckdb;
using namespace std;

static void test_file_system(duckdb_file_system fs, string file_name) {
	REQUIRE(fs != nullptr);

	duckdb_state state = DuckDBSuccess;
	duckdb_file_handle file;

	auto file_path = TestDirectoryPath() + "/" + file_name;

	auto options = duckdb_create_file_open_options();
	state = duckdb_file_open_options_set_flag(options, DUCKDB_FILE_FLAGS_WRITE, true);
	REQUIRE(state == DuckDBSuccess);
	state = duckdb_file_open_options_set_flag(options, DUCKDB_FILE_FLAGS_READ, true);
	REQUIRE(state == DuckDBSuccess);

	// Try to open non-existing file without create flag
	state = duckdb_file_system_open(fs, file_path.c_str(), options, &file);
	REQUIRE(state != DuckDBSuccess);
	auto error_data = duckdb_file_system_error_data(fs);
	auto error_type = duckdb_error_data_error_type(error_data);
	REQUIRE(error_type == DUCKDB_ERROR_IO);
	duckdb_destroy_error_data(&error_data);

	// Set create flag
	state = duckdb_file_open_options_set_flag(options, DUCKDB_FILE_FLAGS_CREATE, true);
	REQUIRE(state == DuckDBSuccess);

	// Create and open a file
	state = duckdb_file_system_open(fs, file_path.c_str(), options, &file);
	REQUIRE(state == DuckDBSuccess);
	REQUIRE(file != nullptr);

	// Write to the file
	const char *data = "Hello, DuckDB File System!";
	int64_t bytes_written = duckdb_file_handle_write(file, data, strlen(data));
	REQUIRE(bytes_written == (int64_t)strlen(data));
	int64_t position = duckdb_file_handle_tell(file);
	REQUIRE(position == bytes_written);
	int64_t size = duckdb_file_handle_size(file);
	REQUIRE(size == bytes_written);

	// Seek to the beginning
	state = duckdb_file_handle_seek(file, 0);
	REQUIRE(state == DuckDBSuccess);
	position = duckdb_file_handle_tell(file);
	REQUIRE(position == 0);

	// Read from the file
	char buffer[30];
	memset(buffer, 0, sizeof(buffer));
	int64_t bytes_read = duckdb_file_handle_read(file, buffer, sizeof(buffer) - 1);
	REQUIRE(bytes_read == bytes_written);
	REQUIRE(strcmp(buffer, data) == 0);
	position = duckdb_file_handle_tell(file);
	REQUIRE(position == bytes_read);
	size = duckdb_file_handle_size(file);
	REQUIRE(size == bytes_written);

	// Seek to the end
	state = duckdb_file_handle_seek(file, bytes_written);
	REQUIRE(state == DuckDBSuccess);
	position = duckdb_file_handle_tell(file);
	REQUIRE(position == bytes_written);
	size = duckdb_file_handle_size(file);
	REQUIRE(size == bytes_written);

	// Try to read from the end of the file
	memset(buffer, 0, sizeof(buffer));
	bytes_read = duckdb_file_handle_read(file, buffer, sizeof(buffer) - 1);
	REQUIRE(bytes_read == 0); // EOF
	position = duckdb_file_handle_tell(file);
	REQUIRE(position == bytes_written);
	size = duckdb_file_handle_size(file);
	REQUIRE(size == bytes_written);

	// Close the file
	state = duckdb_file_handle_seek(file, 0);
	REQUIRE(state == DuckDBSuccess);
	position = duckdb_file_handle_tell(file);
	REQUIRE(position == 0);
	size = duckdb_file_handle_size(file);
	REQUIRE(size == bytes_written);

	// Free resources
	duckdb_destroy_file_handle(&file);
	duckdb_destroy_file_open_options(&options);
}

TEST_CASE("Test File System in C API", "[capi]") {
	CAPITester tester;
	duckdb_file_system fs;
	duckdb::unique_ptr<CAPIResult> result;

	REQUIRE(tester.OpenDatabase(nullptr));
	duckdb_connection_get_file_system(tester.connection, &fs);
	REQUIRE(fs != nullptr);

	test_file_system(fs, "test_file_capi_1.txt");

	duckdb_destroy_file_system(&fs);
	REQUIRE(fs == nullptr);

	// get a file system from the client context as well
	duckdb_client_context context;
	duckdb_connection_get_client_context(tester.connection, &context);

	duckdb_client_context_get_file_system(context, &fs);
	REQUIRE(fs != nullptr);

	test_file_system(fs, "test_file_capi_2.txt");

	duckdb_destroy_file_system(&fs);
	REQUIRE(fs == nullptr);

	duckdb_destroy_client_context(&context);
}
