#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file/table_function_multi_file_list.hpp"
#include "duckdb/function/table/range.hpp"
#include "duckdb/main/client_context.hpp"

using namespace duckdb;

namespace {
struct FileStructBindData : public TableFunctionData {
	explicit FileStructBindData(idx_t file_count) : file_count(file_count) {
	}
	idx_t file_count;
};

struct FileStructState : public GlobalTableFunctionState {
	idx_t offset = 0;
};

unique_ptr<FunctionData> FileStructBind(ClientContext &, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<Identifier> &names) {
	child_list_t<LogicalType> file_fields {{"filename", LogicalType::VARCHAR}, {"file_size", LogicalType::UBIGINT}};
	return_types.push_back(LogicalType::STRUCT(std::move(file_fields)));
	names.emplace_back("file");
	return_types.push_back(LogicalType::INTEGER);
	names.emplace_back("other");
	return make_uniq<FileStructBindData>(UBigIntValue::Get(input.inputs[0]));
}

unique_ptr<GlobalTableFunctionState> FileStructInit(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<FileStructState>();
}

//! Produces one file per chunk - the last file has no size, so it has no open options other than its path
void FileStructFunction(ClientContext &, TableFunctionInput &input, DataChunk &output) {
	auto &bind_data = input.bind_data->Cast<FileStructBindData>();
	auto &state = input.global_state->Cast<FileStructState>();
	if (state.offset >= bind_data.file_count) {
		return;
	}
	auto file_size = state.offset + 1 == bind_data.file_count ? Value(LogicalType::UBIGINT)
	                                                          : Value::UBIGINT(100 * (state.offset + 1));
	output.SetValue(0, 0,
	                Value::STRUCT({{"filename", Value("file_" + to_string(state.offset))}, {"file_size", file_size}}));
	output.SetValue(1, 0, Value::INTEGER(42));
	output.SetChildCardinality(1);
	state.offset++;
}

TableFunction FileStructTableFunction() {
	return TableFunction("file_structs", {LogicalType::UBIGINT}, FileStructFunction, FileStructBind, FileStructInit);
}

void NullFileFunction(ClientContext &, TableFunctionInput &input, DataChunk &output) {
	output.SetValue(0, 0, Value(LogicalType::VARCHAR));
	output.SetChildCardinality(1);
}

unique_ptr<FunctionData> NullFileBind(ClientContext &, TableFunctionBindInput &, vector<LogicalType> &return_types,
                                      vector<Identifier> &names) {
	return_types.push_back(LogicalType::VARCHAR);
	names.emplace_back("file");
	return make_uniq<TableFunctionData>();
}
} // namespace

TEST_CASE("TableFunctionMultiFileList lists the files a table function produces", "[multifile]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.BeginTransaction();
	auto &context = *con.context;

	SECTION("File structs carry their open options") {
		TableFunctionMultiFileList files(context, FileStructTableFunction(), {Value::UBIGINT(3)});
		// nothing is expanded until files are requested
		auto count = files.GetFileCount(0);
		REQUIRE(count.count == 0);
		REQUIRE(count.type == FileExpansionType::NOT_ALL_FILES_KNOWN);
		REQUIRE(files.GetFirstFile().path == "file_0");
		count = files.GetFileCount(0);
		REQUIRE(count.count == 1);
		REQUIRE(count.type == FileExpansionType::NOT_ALL_FILES_KNOWN);

		auto all_files = files.GetAllFiles();
		REQUIRE(all_files.size() == 3);
		REQUIRE(files.GetExpandResult() == FileExpandResult::MULTIPLE_FILES);
		idx_t file_size;
		REQUIRE(all_files[0].extended_info);
		REQUIRE(all_files[0].extended_info->TryGetOption("file_size", file_size));
		REQUIRE(file_size == 100);
		REQUIRE(all_files[1].path == "file_1");
		REQUIRE(all_files[1].extended_info->TryGetOption("file_size", file_size));
		REQUIRE(file_size == 200);
		// a NULL option is an option that was not specified
		REQUIRE(all_files[2].path == "file_2");
		REQUIRE(!all_files[2].extended_info->TryGetOption("file_size", file_size));
		// without a display callback the files themselves are displayed
		REQUIRE(files.GetDisplayFileList(2).size() == 2);
	}
	SECTION("A function that produces no files") {
		TableFunctionMultiFileList files(context, FileStructTableFunction(), {Value::UBIGINT(0)});
		REQUIRE(files.IsEmpty());
		REQUIRE(files.GetTotalFileCount() == 0);
	}
	SECTION("The glob function produces the same files with and without scanning them directly") {
		auto &fs = FileSystem::GetFileSystem(context);
		auto dir = TestCreatePath("table_function_file_list");
		TestCreateDirectory(dir);
		for (idx_t i = 0; i < 5; i++) {
			auto handle = fs.OpenFile(fs.JoinPath(dir, "file_" + to_string(4 - i) + ".csv"),
			                          FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
		}
		auto glob = fs.JoinPath(dir, "*.csv");
		TableFunctionMultiFileList chunk_files(context, GlobTableFunction::GetFunction(), {Value(glob)});
		auto direct_files =
		    GlobTableFunction::CreateFileList(context, {OpenFileInfo(glob)}, FileGlobOptions::DISALLOW_EMPTY);
		auto from_chunks = chunk_files.GetAllFiles();
		auto from_scan = direct_files->GetAllFiles();
		REQUIRE(from_chunks.size() == 5);
		REQUIRE(from_scan.size() == 5);
		for (idx_t i = 0; i < 5; i++) {
			REQUIRE(from_chunks[i].path == fs.JoinPath(dir, "file_" + to_string(i) + ".csv"));
			REQUIRE(from_scan[i].path == from_chunks[i].path);
			// the files are handed over directly - together with what the glob learned about them
			REQUIRE(from_scan[i].extended_info);
			REQUIRE(from_scan[i].extended_info->options.count("file_size"));
		}
		// the glob list displays the globs instead of the files
		auto display = direct_files->GetDisplayFileList();
		REQUIRE(display.size() == 1);
		REQUIRE(display[0].path == glob);
	}
	SECTION("Files must be VARCHAR paths or file structs") {
		auto range_function = TableFunction("range_files", {LogicalType::BIGINT}, NullFileFunction, NullFileBind);
		TableFunctionMultiFileList null_files(context, range_function, {Value::BIGINT(1)});
		REQUIRE_THROWS_WITH(null_files.GetFirstFile(), Catch::Contains("produced a NULL file"));

		vector<LogicalType> integer_types {LogicalType::INTEGER};
		REQUIRE_THROWS_WITH(TableFunctionMultiFileList(context, FileStructTableFunction(),
		                                               make_uniq<FileStructBindData>(1), integer_types),
		                    Catch::Contains("its first column must be a VARCHAR path or a file struct"));
	}
}
