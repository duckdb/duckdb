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
	struct CustomFunctionData : public TableFunctionData {
		CustomFunctionData() {
		}
		vector<int> row_sums;
		idx_t current_idx = 0;
	};

	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names) {
		auto result = make_unique<ThrottlingSum::CustomFunctionData>();
		return_types.emplace_back(LogicalType::INTEGER);
		names.emplace_back("total");
		return move(result);
	}

	static OperatorResultType Function(ExecutionContext &context, TableFunctionInput &data_p, DataChunk &input,
	                                   DataChunk &output) {
		auto &state = (ThrottlingSum::CustomFunctionData &)*data_p.bind_data;

		for (idx_t row_idx = 0; row_idx < input.size(); row_idx++) {
			int sum = 0;
			for (idx_t col_idx = 0; col_idx < input.ColumnCount(); col_idx++) {
				if (input.data[col_idx].GetType() == LogicalType::INTEGER) {
					sum += input.data[col_idx].GetValue(row_idx).GetValue<int>();
				}
			}
			state.row_sums.push_back(sum);
		}

		if (state.current_idx < state.row_sums.size()) {
			output.SetCardinality(1);
			output.SetValue(0, 0, Value(state.row_sums[state.current_idx++]));
		} else {
			output.SetCardinality(0);
		}

		return OperatorResultType::NEED_MORE_INPUT;
	}

	static OperatorFinalizeResultType Finalize(ExecutionContext &context, TableFunctionInput &data_p,
	                                           DataChunk &output) {
		auto &state = (ThrottlingSum::CustomFunctionData &)*data_p.bind_data;

		if (state.current_idx < state.row_sums.size()) {
			output.SetCardinality(1);
			output.SetValue(0, 0, Value(state.row_sums[state.current_idx++]));
			return OperatorFinalizeResultType::HAVE_MORE_OUTPUT;
		} else {
			output.SetCardinality(0);
			return OperatorFinalizeResultType::FINISHED;
		}
	}

	static void Register(Connection &con) {
		// Create our test TableFunction
		con.BeginTransaction();
		auto &client_context = *con.context;
		auto &catalog = Catalog::GetCatalog(client_context);
		TableFunction caching_table_in_out("throttling_sum", {LogicalType::TABLE}, nullptr, ThrottlingSum::Bind);
		caching_table_in_out.in_out_function = ThrottlingSum::Function;
		caching_table_in_out.in_out_function_final = ThrottlingSum::Finalize;
		CreateTableFunctionInfo caching_table_in_out_info(caching_table_in_out);
		catalog.CreateTableFunction(*con.context, &caching_table_in_out_info);
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

	// Check stream result
	auto result =
	    con.SendQuery("SELECT * FROM throttling_sum((select i::INTEGER, (i+1)::INTEGER as j from range(0,3) tbl(i)));");
	REQUIRE_NO_FAIL(*result);

	auto chunk = result->Fetch();
	REQUIRE(chunk);
	REQUIRE(chunk->size() == 1);
	REQUIRE(chunk->data[0].GetValue(0).GetValue<int>() == 1);

	chunk = result->Fetch();
	REQUIRE(chunk);
	REQUIRE(chunk->size() == 1);
	REQUIRE(chunk->data[0].GetValue(0).GetValue<int>() == 3);

	chunk = result->Fetch();
	REQUIRE(chunk);
	REQUIRE(chunk->size() == 1);
	REQUIRE(chunk->data[0].GetValue(0).GetValue<int>() == 5);

	chunk = result->Fetch();
	REQUIRE(!chunk);

	// Large result into aggregation
	auto result3 = con.Query(
	    "SELECT sum(total) FROM throttling_sum((select i::INTEGER, (i+1)::INTEGER as j from range(0,130000) tbl(i)));");
	REQUIRE(result3->ColumnCount() == 1);
	REQUIRE(CHECK_COLUMN(result3, 0, {Value::BIGINT(16900000000)}));
}
