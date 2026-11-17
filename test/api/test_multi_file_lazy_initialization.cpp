#include "catch.hpp"
#include "duckdb/common/multi_file/table_function_multi_file.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

namespace {

class InitializationFileList : public LazyMultiFileList {
public:
	explicit InitializationFileList(idx_t file_count_p) : LazyMultiFileList(nullptr), file_count(file_count_p) {
	}

protected:
	bool ExpandNextPath() const override {
		if (expanded_files.size() == file_count) {
			return false;
		}
		expanded_files.emplace_back("file_" + to_string(expanded_files.size()));
		return true;
	}

private:
	const idx_t file_count;
};

struct InitializationReaderState : public GlobalTableFunctionState {
	explicit InitializationReaderState(const MultiFileList &file_list_p) : file_list(file_list_p) {
	}

	const MultiFileList &file_list;
};

unique_ptr<GlobalTableFunctionState> InitializeLazyReader(ClientContext &, TableFunctionInitInput &input) {
	auto &file_input = TableFunctionFileInitInput::Get(input);
	REQUIRE(file_input.file_list);
	REQUIRE(file_input.file_index.GetIndex() == 0);
	return make_uniq<InitializationReaderState>(*file_input.file_list);
}

} // namespace

TEST_CASE("Multi-file initialization preserves lazy file expansion", "[api][multi_file]") {
	DuckDB db(nullptr);
	Connection con(db);
	InitializationFileList files(4);
	SECTION("No files expanded") {
	}
	SECTION("Some files expanded") {
		files.GetFileCount(2);
	}
	SECTION("All files expanded") {
		files.GetTotalFileCount();
	}
	auto before = files.GetFileCount();

	TableFunction function;
	function.init_global = InitializeLazyReader;
	TableFunctionMultiFileWrapper wrapper(function, TableFunctionMultiFileSettings {});
	MultiFileBindData bind_data;
	MultiFileGlobalState scan_state(files);
	auto state = wrapper.InitializeGlobalState(*con.context, bind_data, scan_state);
	REQUIRE(state);

	TableFunctionFileReader reader(function, OpenFileInfo("file_0"), {}, {});
	reader.file_list_idx = 0;
	reader.PrepareReader(*con.context, *state);
	auto &reader_state = reader.GetFunctionState()->Cast<InitializationReaderState>();
	REQUIRE(&reader_state.file_list == &files);

	auto after = files.GetFileCount();
	REQUIRE(after.count == before.count);
	REQUIRE(after.type == before.type);

	auto count = reader_state.file_list.GetFileCount(3);
	REQUIRE(count.count >= 3);
	REQUIRE(reader_state.file_list.GetTotalFileCount() == 4);
}

TEST_CASE("CSV initialization only expands the file count it needs", "[api][multi_file]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	auto path = TestCreatePath("lazy_initialization.csv");
	REQUIRE_NO_FAIL(con.Query("COPY (SELECT 42 AS i) TO '" + path + "' (FORMAT CSV, HEADER)"));

	bool store_rejects = false;
	SECTION("Parallelism uses a bounded count") {
	}
	SECTION("Rejects use a bounded count") {
		store_rejects = true;
	}

	con.BeginTransaction();
	auto plan = con.ExtractPlan("SELECT i FROM read_csv('" + path +
	                            "', auto_detect=false, header=true, columns={'i':'INTEGER'}, store_rejects=" +
	                            string(store_rejects ? "true" : "false") + ")");
	optional_ptr<LogicalOperator> op = plan.get();
	while (op->type != LogicalOperatorType::LOGICAL_GET) {
		REQUIRE(op->children.size() == 1);
		op = op->children[0].get();
	}
	auto &bind_data = op->Cast<LogicalGet>().bind_data->Cast<MultiFileBindData>();
	REQUIRE(bind_data.initial_reader);

	InitializationFileList files(8);
	MultiFileGlobalState scan_state(files);
	auto state = bind_data.interface->InitializeGlobalState(*con.context, bind_data, scan_state);
	REQUIRE(files.GetFileCount().count == 0);
	bind_data.initial_reader->PrepareReader(*con.context, *state);

	auto count = files.GetFileCount();
	REQUIRE(count.count == 3);
	REQUIRE(count.type == FileExpansionType::NOT_ALL_FILES_KNOWN);
	con.Rollback();
}
