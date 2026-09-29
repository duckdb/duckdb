#include "catch.hpp"
#include "duckdb_cpp.hpp"
#include "test_helpers.hpp"

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

#include <fstream>

// ---------------------------------------------------------------------------
// The column identifiers and file metadata a table function written against the stable C++ API reports for the file
// it reads reach the multi-file reader of a MultiFileFunction wrapping it: a custom multi-file reader - as other
// extensions implement them - maps the columns by field id and reads the metadata of every file.
// ---------------------------------------------------------------------------

namespace {

namespace cxx = duckdb::cxx;

// cpp_pairs_file(path): the file holds lines "<id>,<name>". Columns: id BIGINT (field id 1) and
// detail STRUCT(name VARCHAR (field id 3), id_times_ten BIGINT (field id 4)) (field id 2).
struct PairsBind {
	std::vector<std::pair<int64_t, std::string>> rows;
};

struct PairsGlobal {
	bool done = false;
};

void PairsFileBind(cxx::TableFunction::BindInput &input) {
	auto ctx = input.GetContext();
	auto path = std::string(input.GetArgument(0).Get<cxx::varchar_t>().view());
	PairsBind bind;
	std::ifstream in(path);
	std::string line;
	while (std::getline(in, line)) {
		auto comma = line.find(',');
		bind.rows.emplace_back(std::stoll(line.substr(0, comma)), line.substr(comma + 1));
	}
	input.AddResultColumn("id", ctx.ParseType("BIGINT"));
	input.AddResultColumn("detail", ctx.ParseType("STRUCT(name VARCHAR, id_times_ten BIGINT)"));
	input.SetColumnIdentifier(0, cxx::Value::Create(ctx, int32_t {1}));
	input.SetColumnIdentifier(1, cxx::Value::Create(ctx, int32_t {2}));
	input.SetColumnIdentifier(1, {0}, cxx::Value::Create(ctx, int32_t {3}));
	input.SetColumnIdentifier(1, {1}, cxx::Value::Create(ctx, int32_t {4}));
	input.AddFileMetadata("row_count", cxx::Value::Create(ctx, cxx::varchar_t(std::to_string(bind.rows.size()))));
	input.SetBindData<PairsBind>(std::move(bind));
}

void PairsFileInitGlobal(cxx::TableFunction::InitGlobalInput &input) {
	input.SetGlobalState<PairsGlobal>();
}

void PairsFileExec(cxx::TableFunction::ExecInput &input) {
	const auto &bind = input.GetBindData<PairsBind>();
	auto &global = input.GetGlobalState<PairsGlobal>();
	auto chunk = input.GetOutputChunk();
	if (global.done) {
		return;
	}
	global.done = true;
	auto count = bind.rows.size();
	for (duckdb::idx_t col = 0; col < input.GetColumnCount(); col++) {
		auto vec = chunk.GetVector(col);
		vec.SetSize(count);
		if (input.GetColumnIndex(col) == 0) {
			auto ids = vec.GetDataMutable<int64_t>();
			for (duckdb::idx_t i = 0; i < count; i++) {
				ids[i] = bind.rows[i].first;
			}
			continue;
		}
		auto names = vec.GetChild(0);
		auto tens = vec.GetChild(1).GetDataMutable<int64_t>();
		for (duckdb::idx_t i = 0; i < count; i++) {
			names.AssignString(i, bind.rows[i].second);
			tens[i] = 10 * bind.rows[i].first;
		}
	}
	chunk.GetVector(0).SetSize(count);
}

struct FieldIdScanInfo : public duckdb::TableFunctionInfo {
	duckdb::mutex lock;
	duckdb::vector<std::string> row_counts;
};

//! Reads the columns by field id, in another order and under other names than the file has them
struct FieldIdMultiFileReader : public duckdb::MultiFileReader {
	explicit FieldIdMultiFileReader(duckdb::shared_ptr<duckdb::TableFunctionInfo> info) : info(std::move(info)) {
	}

	static duckdb::unique_ptr<duckdb::MultiFileReader> CreateInstance(const duckdb::BoundTableFunction &table) {
		return duckdb::make_uniq<FieldIdMultiFileReader>(table.function_info);
	}

	bool Bind(duckdb::MultiFileOptions &options, duckdb::MultiFileList &files,
	          duckdb::vector<duckdb::LogicalType> &return_types, duckdb::vector<duckdb::Identifier> &names,
	          duckdb::MultiFileReaderBindData &bind_data) override {
		using duckdb::LogicalType;
		using duckdb::MultiFileColumnDefinition;
		using duckdb::Value;
		duckdb::vector<MultiFileColumnDefinition> schema;

		MultiFileColumnDefinition info_column(
		    "info", LogicalType::STRUCT({{"tenfold", LogicalType::BIGINT}, {"label", LogicalType::VARCHAR}}));
		info_column.identifier = Value::INTEGER(2);
		MultiFileColumnDefinition tenfold("tenfold", LogicalType::BIGINT);
		tenfold.identifier = Value::INTEGER(4);
		MultiFileColumnDefinition label("label", LogicalType::VARCHAR);
		label.identifier = Value::INTEGER(3);
		info_column.children.push_back(tenfold);
		info_column.children.push_back(label);
		schema.push_back(info_column);

		MultiFileColumnDefinition key("key", LogicalType::BIGINT);
		key.identifier = Value::INTEGER(1);
		schema.push_back(key);

		MultiFileColumnDefinition missing("missing", LogicalType::INTEGER);
		missing.identifier = Value::INTEGER(42);
		missing.default_expression = duckdb::make_uniq<duckdb::CastExpression>(
		    LogicalType::INTEGER, duckdb::make_uniq<duckdb::ConstantExpression>(duckdb::Literal::Null()));
		schema.push_back(missing);

		for (auto &column : schema) {
			return_types.push_back(column.type);
			names.push_back(column.name);
		}
		bind_data.schema = std::move(schema);
		bind_data.mapping = duckdb::MultiFileColumnMappingMode::BY_FIELD_ID;
		return true;
	}

	duckdb::ReaderInitializeType
	InitializeReader(duckdb::MultiFileReaderData &reader_data, const duckdb::MultiFileBindData &bind_data,
	                 const duckdb::vector<duckdb::MultiFileColumnDefinition> &global_columns,
	                 const duckdb::vector<duckdb::ColumnIndex> &global_column_ids,
	                 duckdb::optional_ptr<duckdb::TableFilterSet> table_filters, duckdb::ClientContext &context,
	                 duckdb::MultiFileGlobalState &gstate) override {
		auto metadata = reader_data.reader->GetMetadata();
		auto entry = metadata.find("row_count");
		{
			auto &scan_info = info->Cast<FieldIdScanInfo>();
			duckdb::lock_guard<duckdb::mutex> guard(scan_info.lock);
			scan_info.row_counts.push_back(entry == metadata.end() ? "<missing>" : entry->second.ToString());
		}
		return MultiFileReader::InitializeReader(reader_data, bind_data, global_columns, global_column_ids,
		                                         table_filters, context, gstate);
	}

	duckdb::shared_ptr<duckdb::TableFunctionInfo> info;
};

} // namespace

TEST_CASE("Stable C++API: multi-file readers map the files of a MultiFileFunction by field id", "[cpp_api]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);

	// register the function and its multi-file wrapper through the stable C++ API
	{
		// the connection handle of the C API is the connection itself
		auto conn = cxx::Connection::FromOpaque(&con);
		auto function = cxx::TableFunction::Create(conn);
		function.SetName("cpp_pairs_file");
		function.GetSignature().AddParameter("path", conn.ParseType("VARCHAR"));
		function.SetBindCallback(PairsFileBind)
		    .SetInitGlobalCallback(PairsFileInitGlobal)
		    .SetExecCallback(PairsFileExec)
		    .SetProjectionPushdown(true);
		function.Register();
		auto multi_file = cxx::MultiFileFunction::Create(conn);
		multi_file.SetName("cpp_pairs").SetSingleFileFunction("cpp_pairs_file");
		multi_file.Register();
	}

	auto path = duckdb::TestCreatePath("cpp_pairs.txt");
	{
		std::ofstream out(path);
		out << "1,one\n2,two\n3,three\n";
	}
	// read by name, as usual
	auto result = con.Query("SELECT id, detail.name FROM cpp_pairs('" + path + "') ORDER BY id");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {"one", "two", "three"}));

	// read by field id, the way an extension implementing a custom multi-file reader binds the function
	con.BeginTransaction();
	auto &context = *con.context;
	auto &instance = duckdb::DatabaseInstance::GetDatabase(context);
	auto data = duckdb::CatalogTransaction::GetSystemTransaction(instance);
	auto &schema = duckdb::Catalog::GetSystemCatalog(instance).GetSchema(data, duckdb::Identifier::DefaultSchema());
	auto entry = schema.GetEntry(data, duckdb::CatalogType::TABLE_FUNCTION_ENTRY, "cpp_pairs");
	REQUIRE(entry);
	auto function = *entry->Cast<duckdb::TableFunctionCatalogEntry>().functions.functions[0];

	duckdb::vector<duckdb::Value> inputs {duckdb::Value(path)};
	duckdb::named_argument_map_t named_parameters;
	duckdb::vector<duckdb::LogicalType> input_types;
	duckdb::vector<duckdb::Identifier> input_names;
	duckdb::TableFunctionRef ref;
	auto scan_info = duckdb::make_shared_ptr<FieldIdScanInfo>();
	duckdb::TableFunction scan_function;
	scan_function.name = duckdb::Identifier("field_id_scan");
	scan_function.get_multi_file_reader = FieldIdMultiFileReader::CreateInstance;
	scan_function.function_info = scan_info;
	duckdb::BoundTableFunction bound_scan_function(scan_function);

	// without the function info of the multi-file function, it cannot be bound
	{
		duckdb::TableFunctionBindInput bind_input(inputs, named_parameters, input_types, input_names, nullptr, nullptr,
		                                          bound_scan_function, ref);
		duckdb::vector<duckdb::LogicalType> types;
		duckdb::vector<duckdb::Identifier> names;
		REQUIRE_THROWS_AS(function.bind(context, bind_input, types, names), duckdb::InvalidInputException);
	}

	duckdb::TableFunctionBindInput bind_input(inputs, named_parameters, input_types, input_names,
	                                          function.function_info.get(), nullptr, bound_scan_function, ref);
	duckdb::vector<duckdb::LogicalType> types;
	duckdb::vector<duckdb::Identifier> names;
	auto bind_data = function.bind(context, bind_input, types, names);
	REQUIRE(names.size() == 3);

	duckdb::vector<duckdb::column_t> column_ids {0, 1, 2};
	duckdb::TableFunctionInitInput init_input(bind_data.get(), column_ids, duckdb::vector<duckdb::idx_t>(), nullptr);
	auto global_state = function.init_global(context, init_input);
	duckdb::ThreadContext thread_context(context);
	duckdb::ExecutionContext execution_context(context, thread_context, nullptr);
	auto local_state = function.init_local(execution_context, init_input, global_state.get());

	duckdb::DataChunk chunk;
	chunk.Initialize(context, types);
	duckdb::vector<std::string> rows;
	while (true) {
		chunk.Reset();
		duckdb::TableFunctionInput function_input(bind_data.get(), local_state.get(), global_state.get());
		function.function(context, function_input, chunk);
		if (chunk.size() == 0) {
			break;
		}
		for (duckdb::idx_t r = 0; r < chunk.size(); r++) {
			rows.push_back(chunk.GetValue(0, r).ToString() + " | " + chunk.GetValue(1, r).ToString() + " | " +
			               chunk.GetValue(2, r).ToString());
		}
	}
	con.Commit();

	REQUIRE(rows == duckdb::vector<std::string> {"{'tenfold': 10, 'label': one} | 1 | NULL",
	                                             "{'tenfold': 20, 'label': two} | 2 | NULL",
	                                             "{'tenfold': 30, 'label': three} | 3 | NULL"});
	REQUIRE(scan_info->row_counts == duckdb::vector<std::string> {"3"});
}
