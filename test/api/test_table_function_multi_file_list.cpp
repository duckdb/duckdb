#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file/table_function_multi_file_list.hpp"
#include "duckdb/main/client_context.hpp"

using namespace duckdb;

TEST_CASE("TableFunctionMultiFileList lists the files a table function produces", "[multifile]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.BeginTransaction();
	auto &context = *con.context;

	auto &fs = FileSystem::GetFileSystem(context);
	auto dir = TestCreatePath("table_function_file_list");
	TestCreateDirectory(dir);
	for (idx_t i = 0; i < 5; i++) {
		auto handle = fs.OpenFile(fs.JoinPath(dir, "file_" + to_string(4 - i) + ".csv"),
		                          FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
	}
	auto glob = fs.JoinPath(dir, "*.csv");

	SECTION("Files are listed lazily from the output of the function") {
		TableFunctionMultiFileList files(context, Identifier("glob"), {Value(glob)});
		auto count = files.GetFileCount(0);
		REQUIRE(count.count == 0);
		REQUIRE(count.type == FileExpansionType::NOT_ALL_FILES_KNOWN);
		auto all_files = files.GetAllFiles();
		REQUIRE(all_files.size() == 5);
		for (idx_t i = 0; i < 5; i++) {
			REQUIRE(all_files[i].path == fs.JoinPath(dir, "file_" + to_string(i) + ".csv"));
			// glob only produces the paths
			REQUIRE(!all_files[i].extended_info);
		}
		// the list displays the files the function was called with
		auto display = files.GetDisplayFileList();
		REQUIRE(display.size() == 1);
		REQUIRE(display[0].path == glob);
	}
	SECTION("A MAP column holds the options to open the files with") {
		named_argument_map_t parameters;
		parameters["full_file_info"] = Value::BOOLEAN(true);
		TableFunctionMultiFileList files(context, Identifier("glob"),
		                                 {Value::LIST(LogicalType::VARCHAR, {Value(glob)})}, std::move(parameters));
		auto all_files = files.GetAllFiles();
		REQUIRE(all_files.size() == 5);
		for (auto &file : all_files) {
			REQUIRE(file.extended_info);
			idx_t file_size;
			REQUIRE(file.extended_info->TryGetOption("file_size", file_size));
			REQUIRE(file_size == 0);
			REQUIRE(file.extended_info->options.at("file_size").type() == LogicalType::BIGINT);
			REQUIRE(file.extended_info->options.at("etag").type() == LogicalType::BLOB);
		}
	}
	SECTION("The first column must hold files") {
		REQUIRE_THROWS_WITH(TableFunctionMultiFileList(context, Identifier("test_all_types"), {}),
		                    Catch::Contains("its first column must be a VARCHAR path or a file struct"));
		// a function with a local state cannot be run outside of a pipeline
		REQUIRE_THROWS_WITH(TableFunctionMultiFileList(context, Identifier("range"), {Value::BIGINT(3)}),
		                    Catch::Contains("cannot provide a file list"));
		REQUIRE_THROWS_WITH(TableFunctionMultiFileList(context, Identifier("no_such_function"), {Value(glob)}),
		                    Catch::Contains("no_such_function"));
	}
}
