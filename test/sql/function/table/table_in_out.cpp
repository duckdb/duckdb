#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

using namespace duckdb;
using namespace std;

// Dummy TableInOutFunction that:
// - sums all INTEGER values in each row
// - only emits 1 row per call to ThrottlingSum::Function, caching the remainder
// - during flushing of caching operators still emits only 1 row sum per call, meaning that multiple flushes are
// required to correctly process this operator
struct ThrottlingSum {
	struct ThrottlingSumLocalData : public LocalTableFunctionState {
		ThrottlingSumLocalData() {
		}
		duckdb::vector<int> row_sums;
		idx_t current_idx = 0;
	};

	static duckdb::unique_ptr<GlobalTableFunctionState> ThrottlingSumGlobalInit(ClientContext &context,
	                                                                            TableFunctionInitInput &input) {
		return make_uniq<GlobalTableFunctionState>();
	}

	static duckdb::unique_ptr<LocalTableFunctionState> ThrottlingSumLocalInit(ExecutionContext &context,
	                                                                          TableFunctionInitInput &input,
	                                                                          GlobalTableFunctionState *global_state) {
		return make_uniq<ThrottlingSumLocalData>();
	}

	static duckdb::unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                             duckdb::vector<LogicalType> &return_types,
	                                             duckdb::vector<string> &names) {
		return_types.emplace_back(LogicalType::INTEGER);
		names.emplace_back("total");
		return make_uniq<TableFunctionData>();
	}

	static OperatorResultType Function(ExecutionContext &context, TableFunctionInput &data_p, DataChunk &input,
	                                   DataChunk &output) {
		auto &local_state = data_p.local_state->Cast<ThrottlingSum::ThrottlingSumLocalData>();

		for (idx_t row_idx = 0; row_idx < input.size(); row_idx++) {
			int sum = 0;
			for (idx_t col_idx = 0; col_idx < input.ColumnCount(); col_idx++) {
				if (input.data[col_idx].GetType() == LogicalType::INTEGER) {
					sum += input.data[col_idx].GetValue(row_idx).GetValue<int>();
				}
			}
			local_state.row_sums.push_back(sum);
		}

		if (PhysicalOperator::OperatorCachingAllowed(context)) {
			// Caching is allowed
			if (local_state.current_idx < local_state.row_sums.size()) {
				output.SetCardinality(1);
				output.SetValue(0, 0, Value(local_state.row_sums[local_state.current_idx++]));
			} else {
				output.SetCardinality(0);
			}
		} else {
			// Caching is not allowed, we should emit everything!
			auto to_emit = local_state.row_sums.size() - local_state.current_idx;
			for (idx_t i = 0; i < to_emit; i++) {
				output.SetValue(0, i, Value(local_state.row_sums[local_state.current_idx + i]));
			}
			local_state.current_idx += to_emit;
			output.SetCardinality(to_emit);
		}

		return OperatorResultType::NEED_MORE_INPUT;
	}

	static OperatorFinalizeResultType Finalize(ExecutionContext &context, TableFunctionInput &data_p,
	                                           DataChunk &output) {
		auto &local_state = data_p.local_state->Cast<ThrottlingSum::ThrottlingSumLocalData>();

		if (local_state.current_idx < local_state.row_sums.size()) {
			output.SetCardinality(1);
			output.SetValue(0, 0, Value(local_state.row_sums[local_state.current_idx++]));
			return OperatorFinalizeResultType::HAVE_MORE_OUTPUT;
		} else {
			return OperatorFinalizeResultType::FINISHED;
		}
	}

	static void Register(Connection &con) {
		// Create our test TableFunction
		con.BeginTransaction();
		auto &client_context = *con.context;
		auto &catalog = Catalog::GetSystemCatalog(client_context);
		TableFunction caching_table_in_out("throttling_sum", {LogicalType::TABLE}, nullptr, ThrottlingSum::Bind,
		                                   ThrottlingSum::ThrottlingSumGlobalInit,
		                                   ThrottlingSum::ThrottlingSumLocalInit);
		caching_table_in_out.in_out_function = ThrottlingSum::Function;
		caching_table_in_out.in_out_function_final = ThrottlingSum::Finalize;
		CreateTableFunctionInfo caching_table_in_out_info(caching_table_in_out);
		catalog.CreateTableFunction(*con.context, caching_table_in_out_info);
		con.Commit();
	}
};

TEST_CASE("Caching TableInOutFunction", "[filter][.]") {
	DuckDB db(nullptr);
	Connection con(db);

	ThrottlingSum::Register(con);

	// Check result
	auto result2 =
	    con.Query("SELECT * FROM throttling_sum((select i::INTEGER, (i+1)::INTEGER as j from range(0,3) tbl(i)));");
	REQUIRE(result2->ColumnCount() == 1);
	REQUIRE(CHECK_COLUMN(result2, 0, {1, 3, 5}));

	// TODO: streaming these is currently unsupported

	// Large result into aggregation
	auto result3 = con.Query(
	    "SELECT sum(total) FROM throttling_sum((select i::INTEGER, (i+1)::INTEGER as j from range(0,130000) tbl(i)));");
	REQUIRE(result3->ColumnCount() == 1);
	REQUIRE(CHECK_COLUMN(result3, 0, {Value::BIGINT(16900000000)}));
}

TEST_CASE("Parallel execution with caching table in out functions", "[filter][.]") {
	DuckDB db(nullptr);
	Connection con(db);

	ThrottlingSum::Register(con);

	auto result = con.Query("CREATE TABLE test_data as select i::INTEGER from range(0,200000) tbl(i);");
	auto result2 = con.Query("SELECT * FROM throttling_sum((select * from test_data));");

	REQUIRE(result2->ColumnCount() == 1);
	REQUIRE(result2->RowCount() == 200000);
	REQUIRE(CHECK_COLUMN(result2, 0, {0, 1, 2, 3, 4, 5}));
}
