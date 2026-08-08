#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

using namespace duckdb;

struct LimitHintProbe {
	struct BindData : public TableFunctionData {
		idx_t max_rows = 0;
	};

	struct GlobalState : public GlobalTableFunctionState {
		optional_idx limit;

		idx_t MaxThreads() const override {
			return 8;
		}
	};

	struct LocalState : public LocalTableFunctionState {
		optional_idx limit;
		idx_t row_idx = 0;
	};

	static duckdb::unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                             duckdb::vector<LogicalType> &return_types,
	                                             duckdb::vector<duckdb::string> &names) {
		auto result = make_uniq<BindData>();
		result->max_rows = NumericCast<idx_t>(input.inputs[0].GetValue<int64_t>());

		return_types.emplace_back(LogicalType::BIGINT);
		names.emplace_back("row_idx");
		return_types.emplace_back(LogicalType::BIGINT);
		names.emplace_back("global_limit");
		return_types.emplace_back(LogicalType::BIGINT);
		names.emplace_back("local_limit");

		return std::move(result);
	}

	static duckdb::unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &context,
	                                                               TableFunctionInitInput &input) {
		auto result = make_uniq<GlobalState>();
		result->limit = input.limit;
		return std::move(result);
	}

	static duckdb::unique_ptr<LocalTableFunctionState>
	InitLocal(ExecutionContext &context, TableFunctionInitInput &input, GlobalTableFunctionState *global_state) {
		auto result = make_uniq<LocalState>();
		result->limit = input.limit;
		return std::move(result);
	}

	static void SetLimitValue(Vector &result, idx_t row_idx, optional_idx limit) {
		if (limit.IsValid()) {
			result.SetValue(row_idx, Value::BIGINT(NumericCast<int64_t>(limit.GetIndex())));
		} else {
			result.SetValue(row_idx, Value(LogicalType::BIGINT));
		}
	}

	static void Function(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
		auto &bind = data_p.bind_data->Cast<BindData>();
		auto &global_state = data_p.global_state->Cast<GlobalState>();
		auto &local_state = data_p.local_state->Cast<LocalState>();

		if (local_state.row_idx >= bind.max_rows) {
			return;
		}

		auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, bind.max_rows - local_state.row_idx);
		output.SetChildCardinality(count);
		for (idx_t idx = 0; idx < count; idx++) {
			output.data[0].SetValue(idx, Value::BIGINT(NumericCast<int64_t>(local_state.row_idx + idx)));
			SetLimitValue(output.data[1], idx, global_state.limit);
			SetLimitValue(output.data[2], idx, local_state.limit);
		}
		local_state.row_idx += count;
	}

	static void Register(Connection &con) {
		con.BeginTransaction();
		auto &catalog = Catalog::GetSystemCatalog(*con.context);
		TableFunction function("limit_hint_probe", {LogicalType::BIGINT}, LimitHintProbe::Function,
		                       LimitHintProbe::Bind, LimitHintProbe::InitGlobal, LimitHintProbe::InitLocal);
		CreateTableFunctionInfo info(function);
		catalog.CreateTableFunction(*con.context, info);
		con.Commit();
	}
};

static const PhysicalTableScan *FindTableScan(const PhysicalOperator &op) {
	if (op.type == PhysicalOperatorType::TABLE_SCAN) {
		return &op.Cast<PhysicalTableScan>();
	}
	for (auto &child : op.GetChildren()) {
		auto result = FindTableScan(child.get());
		if (result) {
			return result;
		}
	}
	return nullptr;
}

TEST_CASE("Table function limit pushdown", "[table_function]") {
	DuckDB db(nullptr);
	Connection con(db);
	LimitHintProbe::Register(con);

	auto no_limit = con.Query("SELECT global_limit IS NULL, local_limit IS NULL FROM limit_hint_probe(1)");
	REQUIRE_NO_FAIL(*no_limit);
	REQUIRE(CHECK_COLUMN(no_limit, 0, {true}));
	REQUIRE(CHECK_COLUMN(no_limit, 1, {true}));

	auto simple_limit = con.Query("SELECT global_limit, local_limit FROM limit_hint_probe(10) LIMIT 3");
	REQUIRE_NO_FAIL(*simple_limit);
	REQUIRE(CHECK_COLUMN(simple_limit, 0, {3, 3, 3}));
	REQUIRE(CHECK_COLUMN(simple_limit, 1, {3, 3, 3}));

	auto offset_limit =
	    con.Query("SELECT row_idx, global_limit, local_limit FROM limit_hint_probe(10) LIMIT 3 OFFSET 2");
	REQUIRE_NO_FAIL(*offset_limit);
	REQUIRE(CHECK_COLUMN(offset_limit, 0, {2, 3, 4}));
	REQUIRE(CHECK_COLUMN(offset_limit, 1, {5, 5, 5}));
	REQUIRE(CHECK_COLUMN(offset_limit, 2, {5, 5, 5}));

	auto projection = con.Query("SELECT global_limit + 0, local_limit + 0 FROM limit_hint_probe(10) LIMIT 3");
	REQUIRE_NO_FAIL(*projection);
	REQUIRE(CHECK_COLUMN(projection, 0, {3, 3, 3}));
	REQUIRE(CHECK_COLUMN(projection, 1, {3, 3, 3}));

	auto filter = con.Query(
	    "SELECT global_limit IS NULL, local_limit IS NULL FROM limit_hint_probe(10) WHERE row_idx > 0 LIMIT 3");
	REQUIRE_NO_FAIL(*filter);
	REQUIRE(CHECK_COLUMN(filter, 0, {true, true, true}));
	REQUIRE(CHECK_COLUMN(filter, 1, {true, true, true}));

	auto order = con.Query(
	    "SELECT global_limit IS NULL, local_limit IS NULL FROM limit_hint_probe(10) ORDER BY row_idx LIMIT 3");
	REQUIRE_NO_FAIL(*order);
	REQUIRE(CHECK_COLUMN(order, 0, {true, true, true}));
	REQUIRE(CHECK_COLUMN(order, 1, {true, true, true}));

	auto threshold = con.Query("SELECT bool_and(global_limit = 8192), bool_and(local_limit = 8192), count(*) "
	                           "FROM (SELECT global_limit, local_limit FROM limit_hint_probe(9000) LIMIT 8192)");
	REQUIRE_NO_FAIL(*threshold);
	REQUIRE(CHECK_COLUMN(threshold, 0, {true}));
	REQUIRE(CHECK_COLUMN(threshold, 1, {true}));
	REQUIRE(CHECK_COLUMN(threshold, 2, {8192}));

	auto large_projection = con.Query("SELECT bool_and(global_limit = 9000), bool_and(local_limit = 9000), count(*) "
	                                  "FROM (SELECT global_limit + 0 AS global_limit, local_limit + 0 AS local_limit "
	                                  "FROM limit_hint_probe(10000) LIMIT 9000)");
	REQUIRE_NO_FAIL(*large_projection);
	REQUIRE(CHECK_COLUMN(large_projection, 0, {true}));
	REQUIRE(CHECK_COLUMN(large_projection, 1, {true}));
	REQUIRE(CHECK_COLUMN(large_projection, 2, {9000}));

	auto limited = con.Prepare("SELECT row_idx FROM limit_hint_probe(10) LIMIT 3");
	REQUIRE(!limited->HasError());
	auto limited_scan = FindTableScan(limited->data->physical_plan->Root());
	REQUIRE(limited_scan);
	REQUIRE(limited_scan->ParallelSource());

	auto unlimited = con.Prepare("SELECT row_idx FROM limit_hint_probe(10)");
	REQUIRE(!unlimited->HasError());
	auto unlimited_scan = FindTableScan(unlimited->data->physical_plan->Root());
	REQUIRE(unlimited_scan);
	REQUIRE(unlimited_scan->ParallelSource());
}
