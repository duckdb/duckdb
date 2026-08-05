#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

using namespace duckdb;

// Regression test for projection_ids corruption when a filter is pushed on a column whose type
// is not supported for pushdown (supports_pushdown_type returns false).
//
// The plan generator extracts such filters into a PhysicalFilter on top of the scan. When
// projection_ids is empty (the identity-mapping convention: all column_ids are output columns),
// it must not be appended to - doing so silently rewrites "output all columns" into "output only
// the filter column", dropping the other columns and causing an out-of-bounds access downstream.
//
// The table function below follows the projection-pushdown contract correctly: it honors
// column_ids and sizes its output from projection_ids. Column 1 ("val") reports
// supports_pushdown_type == false, mimicking a custom type (e.g. INET).
struct PushdownProjection {
	struct BindData : public TableFunctionData {};
	struct GlobalState : public GlobalTableFunctionState {
		duckdb::vector<column_t> projected_cols;
		bool done = false;
	};

	static duckdb::unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                             duckdb::vector<LogicalType> &return_types,
	                                             duckdb::vector<string> &names) {
		return_types.emplace_back(LogicalType::INTEGER);
		names.emplace_back("id");
		return_types.emplace_back(LogicalType::VARCHAR);
		names.emplace_back("val");
		return make_uniq<BindData>();
	}

	static duckdb::unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &context,
	                                                               TableFunctionInitInput &input) {
		auto result = make_uniq<GlobalState>();
		// Honor the projection-pushdown contract: empty projection_ids == identity mapping.
		if (input.projection_ids.empty()) {
			for (auto &col_id : input.column_ids) {
				result->projected_cols.push_back(col_id);
			}
		} else {
			for (auto &proj_idx : input.projection_ids) {
				result->projected_cols.push_back(input.column_ids[proj_idx]);
			}
		}
		return std::move(result);
	}

	static void Function(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
		auto &gstate = data_p.global_state->Cast<GlobalState>();
		if (gstate.done) {
			output.SetCardinality(0);
			return;
		}
		for (idx_t out = 0; out < gstate.projected_cols.size(); out++) {
			if (gstate.projected_cols[out] == 0) {
				output.SetValue(out, 0, Value::INTEGER(42));
			} else if (gstate.projected_cols[out] == 1) {
				output.SetValue(out, 0, Value("hello"));
			}
		}
		output.SetCardinality(1);
		gstate.done = true;
	}

	// Column 1 ("val") cannot be pushed down (mimics a custom type such as INET).
	static bool SupportsPushdownType(const FunctionData &bind_data, idx_t col_idx) {
		return col_idx != 1;
	}
};

TEST_CASE("Filter pushdown on unsupported type with multi-column projection", "[tablefunction]") {
	DuckDB db(nullptr);
	Connection con(db);

	con.BeginTransaction();
	auto &catalog = Catalog::GetSystemCatalog(*con.context);
	TableFunction pd("pushdown_projection", {}, PushdownProjection::Function, PushdownProjection::Bind,
	                 PushdownProjection::InitGlobal);
	pd.supports_pushdown_type = PushdownProjection::SupportsPushdownType;
	pd.projection_pushdown = true;
	pd.filter_pushdown = true;
	CreateTableFunctionInfo info(pd);
	catalog.CreateTableFunction(*con.context, info);
	con.Commit();

	// Multi-column projection with a single-element IN filter on the unsupported column.
	auto result = con.Query("SELECT id, val FROM pushdown_projection() WHERE val IN ('hello');");
	REQUIRE(!result->HasError());
	REQUIRE(result->ColumnCount() == 2);
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	REQUIRE(CHECK_COLUMN(result, 1, {"hello"}));

	// Multi-element IN filter.
	result = con.Query("SELECT id, val FROM pushdown_projection() WHERE val IN ('hello', 'world');");
	REQUIRE(!result->HasError());
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	REQUIRE(CHECK_COLUMN(result, 1, {"hello"}));

	// Equality filter on the unsupported column.
	result = con.Query("SELECT id, val FROM pushdown_projection() WHERE val = 'hello';");
	REQUIRE(!result->HasError());
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	REQUIRE(CHECK_COLUMN(result, 1, {"hello"}));

	// Single-column projection of the unsupported column (also exercised the filter path).
	result = con.Query("SELECT val FROM pushdown_projection() WHERE val IN ('hello');");
	REQUIRE(!result->HasError());
	REQUIRE(CHECK_COLUMN(result, 0, {"hello"}));
}
