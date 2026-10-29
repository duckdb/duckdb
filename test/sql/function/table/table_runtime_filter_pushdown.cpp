#include "catch.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/table_filter_set.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

// A table function that, like a scanner of a remote system, receives its filters once in init_global and records
// them as text. It produces k = i * step for i in [0, n) and leaves the (optional) join filters to the join.
struct RecordedFilters : public TableFunctionInfo {
	vector<string> filters;
};

struct FilterRecordingScan {
	struct BindData : public TableFunctionData {
		RecordedFilters *recorded;
		idx_t count;
		int64_t step;
	};
	struct GlobalState : public GlobalTableFunctionState {
		idx_t offset = 0;
	};

	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<Identifier> &names) {
		auto result = make_uniq<BindData>();
		result->recorded = &input.info->Cast<RecordedFilters>();
		result->count = input.inputs[0].GetValue<idx_t>();
		result->step = input.inputs[1].GetValue<int64_t>();
		return_types.emplace_back(LogicalType::BIGINT);
		names.emplace_back("k");
		return std::move(result);
	}

	static unique_ptr<NodeStatistics> Cardinality(ClientContext &context, const FunctionData *bind_data) {
		auto count = bind_data->Cast<BindData>().count;
		return make_uniq<NodeStatistics>(count, count);
	}

	static unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &context, TableFunctionInitInput &input) {
		auto &recorded = *input.bind_data->Cast<BindData>().recorded;
		if (input.filters) {
			for (auto &entry : *input.filters) {
				recorded.filters.push_back(entry.Filter().Cast<ExpressionFilter>().ToString("k"));
			}
		}
		return make_uniq<GlobalState>();
	}

	static void Function(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
		auto &bind_data = data.bind_data->Cast<BindData>();
		auto &state = data.global_state->Cast<GlobalState>();
		auto chunk_size = MinValue<idx_t>(STANDARD_VECTOR_SIZE, bind_data.count - state.offset);
		for (idx_t i = 0; i < chunk_size; i++) {
			output.data[0].SetValue(i, Value::BIGINT(NumericCast<int64_t>(state.offset + i) * bind_data.step));
		}
		state.offset += chunk_size;
		output.SetChildCardinality(chunk_size);
	}

	static shared_ptr<RecordedFilters> Register(Connection &con, const string &name, bool runtime_filter_pushdown) {
		TableFunction function(Identifier(name), {LogicalType::UBIGINT, LogicalType::BIGINT}, Function, Bind,
		                       InitGlobal);
		function.cardinality = Cardinality;
		// dynamic join filters only reach table functions that support projection pushdown
		function.projection_pushdown = true;
		function.filter_pushdown = true;
		function.runtime_filter_pushdown = runtime_filter_pushdown;
		auto recorded = make_shared_ptr<RecordedFilters>();
		function.function_info = recorded;
		con.BeginTransaction();
		CreateTableFunctionInfo info(function);
		Catalog::GetSystemCatalog(*con.context).CreateTableFunction(*con.context, info);
		con.Commit();
		return recorded;
	}
};

static string RecordedFiltersFor(Connection &con, RecordedFilters &recorded, const string &query, idx_t row_count) {
	recorded.filters.clear();
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(NumericCast<int64_t>(row_count))}));
	return StringUtil::Join(recorded.filters, "\n");
}

TEST_CASE("Table functions can decline Bloom and prefix-range join filters", "[tablefunction]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET threads = 1"));
	REQUIRE_NO_FAIL(
	    con.Query("SET disabled_optimizers = 'join_order,build_side_probe_side,compressed_materialization'"));
	// disable IN-list filters, which take precedence over min/max filters
	REQUIRE_NO_FAIL(con.Query("SET dynamic_or_filter_threshold = 0"));
	auto with_runtime = FilterRecordingScan::Register(con, "scan_with_runtime_filters", true);
	auto without_runtime = FilterRecordingScan::Register(con, "scan_without_runtime_filters", false);

	// a dense build side: a prefix-range filter qualifies and subsumes min/max
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE dense_build AS SELECT (1000 + i * 2)::BIGINT AS k FROM range(500) t(i)"));
	const string dense_join = "(100000::UBIGINT, 1) p JOIN dense_build b ON p.k = b.k";

	SECTION("by default the table function receives the prefix-range filter") {
		auto filters =
		    RecordedFiltersFor(con, *with_runtime, "SELECT count(*) FROM scan_with_runtime_filters" + dense_join, 500);
		REQUIRE(StringUtil::Contains(filters, "PRF("));
	}
	SECTION("a declining table function receives min/max instead") {
		auto filters = RecordedFiltersFor(con, *without_runtime,
		                                  "SELECT count(*) FROM scan_without_runtime_filters" + dense_join, 500);
		REQUIRE(StringUtil::Contains(filters, "k >= 1000"));
		REQUIRE(StringUtil::Contains(filters, "k <= 1998"));
		REQUIRE(!StringUtil::Contains(filters, "PRF("));
		REQUIRE(!StringUtil::Contains(filters, "BF("));
	}
	SECTION("a sparse, wide build side would use a Bloom filter") {
		REQUIRE_NO_FAIL(
		    con.Query("CREATE TABLE sparse_build AS SELECT (i * 20000000000)::BIGINT AS k FROM range(100) t(i)"));
		const string sparse_join = "(100000::UBIGINT, 20000000) p JOIN sparse_build b ON p.k = b.k";
		auto filters =
		    RecordedFiltersFor(con, *with_runtime, "SELECT count(*) FROM scan_with_runtime_filters" + sparse_join, 100);
		REQUIRE(StringUtil::Contains(filters, "BF("));
		filters = RecordedFiltersFor(con, *without_runtime,
		                             "SELECT count(*) FROM scan_without_runtime_filters" + sparse_join, 100);
		REQUIRE(StringUtil::Contains(filters, "k >= 0"));
		REQUIRE(StringUtil::Contains(filters, "k <= 1980000000000"));
		REQUIRE(!StringUtil::Contains(filters, "BF("));
	}
	SECTION("an external join honours the declining table function") {
		REQUIRE_NO_FAIL(con.Query("SET debug_force_external = true"));
		auto filters = RecordedFiltersFor(con, *without_runtime,
		                                  "SELECT count(*) FROM scan_without_runtime_filters" + dense_join, 500);
		REQUIRE(StringUtil::Contains(filters, "k >= 1000"));
		REQUIRE(StringUtil::Contains(filters, "k <= 1998"));
		REQUIRE(!StringUtil::Contains(filters, "PRF("));
		REQUIRE(!StringUtil::Contains(filters, "BF("));
	}
	SECTION("a native scan next to a declining table function still receives the prefix-range filter") {
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE native_probe AS SELECT i::BIGINT AS k FROM range(100000) t(i)"));
		const string union_join = "FROM (SELECT k FROM native_probe UNION ALL SELECT k FROM "
		                          "scan_without_runtime_filters(100000::UBIGINT, 1)) p JOIN dense_build b ON p.k = b.k";
		auto filters = RecordedFiltersFor(con, *without_runtime, "SELECT count(*) " + union_join, 1000);
		REQUIRE(StringUtil::Contains(filters, "k >= 1000"));
		REQUIRE(StringUtil::Contains(filters, "k <= 1998"));
		REQUIRE(!StringUtil::Contains(filters, "PRF("));
		auto plan = con.Query("EXPLAIN ANALYZE SELECT count(*) " + union_join);
		REQUIRE_NO_FAIL(*plan);
		REQUIRE(StringUtil::Contains(plan->Collection().GetValue(1, 0).ToString(), "PRF("));
	}
}
