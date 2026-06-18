#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

using namespace duckdb;

struct FilterPushdownFn {
	struct Data : public TableFunctionData {
		bool done = false;
	};

	static duckdb::unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                             duckdb::vector<LogicalType> &return_types,
	                                             duckdb::vector<string> &names) {
		return_types.push_back(LogicalType::INTEGER);
		names.push_back("id");
		return_types.push_back(LogicalType::VARCHAR);
		names.push_back("val");
		return make_uniq<Data>();
	}

	static void Function(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
		auto &data = (Data &)*data_p.bind_data;
		if (!data.done) {
			for (idx_t c = 0; c < output.ColumnCount(); c++) {
				auto &type = output.data[c].GetType();
				if (type == LogicalType::INTEGER) {
					output.SetValue(c, 0, Value(42));
				} else {
					output.SetValue(c, 0, Value("hello"));
				}
			}
			output.SetCardinality(1);
			data.done = true;
		}
	}

	static bool SupportsPushdownType(const FunctionData &bind_data, idx_t col_idx) {
		return col_idx != 1;
	}

	static void Register(Connection &con) {
		con.BeginTransaction();
		auto &context = *con.context;
		auto &catalog = Catalog::GetSystemCatalog(context);
		TableFunction fn("filter_pushdown_test", {}, FilterPushdownFn::Function, FilterPushdownFn::Bind);
		fn.supports_pushdown_type = FilterPushdownFn::SupportsPushdownType;
		fn.projection_pushdown = true;
		CreateTableFunctionInfo info(fn);
		catalog.CreateTableFunction(context, info);
		con.Commit();
	}
};

TEST_CASE("Multi-column projection with IN on unsupported pushdown type", "[tablefunction][filter]") {
	DuckDB db(nullptr);
	Connection con(db);
	FilterPushdownFn::Register(con);

	auto result = con.Query("SELECT id, val FROM filter_pushdown_test() WHERE val IN ('hello');");
	REQUIRE(result->RowCount() == 1);
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	REQUIRE(CHECK_COLUMN(result, 1, {"hello"}));
}

TEST_CASE("Single-column projection with IN on unsupported pushdown type", "[tablefunction][filter]") {
	DuckDB db(nullptr);
	Connection con(db);
	FilterPushdownFn::Register(con);

	auto result = con.Query("SELECT val FROM filter_pushdown_test() WHERE val IN ('hello');");
	REQUIRE(result->RowCount() == 1);
	REQUIRE(CHECK_COLUMN(result, 0, {"hello"}));
}

TEST_CASE("Multi-column projection with = on unsupported pushdown type", "[tablefunction][filter]") {
	DuckDB db(nullptr);
	Connection con(db);
	FilterPushdownFn::Register(con);

	auto result = con.Query("SELECT id, val FROM filter_pushdown_test() WHERE val = 'hello';");
	REQUIRE(result->RowCount() == 1);
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	REQUIRE(CHECK_COLUMN(result, 1, {"hello"}));
}
