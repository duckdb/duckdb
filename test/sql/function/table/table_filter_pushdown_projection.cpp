#include "catch.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

namespace {

//! Ten rows of (id, total, region), or (region, id, total) when called with true. Refuses filters on region.
struct RegionFilterRefusingScan {
	struct BindData : public TableFunctionData {
		BindData(vector<string> names_p, bool projection_pushdown_p)
		    : names(std::move(names_p)), projection_pushdown(projection_pushdown_p) {
		}

		vector<string> names;
		bool projection_pushdown;
	};

	struct GlobalState : public GlobalTableFunctionState {
		vector<column_t> column_ids;
		vector<idx_t> projection_ids;
		idx_t offset = 0;
	};

	static unique_ptr<FunctionData> Bind(ClientContext &, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<Identifier> &names) {
		auto region_first = input.inputs[0].GetValue<bool>();
		vector<string> column_names =
		    region_first ? vector<string> {"region", "id", "total"} : vector<string> {"id", "total", "region"};
		for (auto &name : column_names) {
			return_types.push_back(name == "region" ? LogicalType::VARCHAR : LogicalType::BIGINT);
			names.emplace_back(name);
		}
		return make_uniq<BindData>(std::move(column_names), input.table_function.projection_pushdown);
	}

	static unique_ptr<GlobalTableFunctionState> GlobalInit(ClientContext &, TableFunctionInitInput &input) {
		auto &bind_data = input.bind_data->Cast<BindData>();
		auto state = make_uniq<GlobalState>();
		if (!bind_data.projection_pushdown) {
			state->column_ids = {0, 1, 2};
		} else {
			state->column_ids = input.column_ids;
			if (input.CanRemoveFilterColumns()) {
				state->projection_ids = input.projection_ids;
			}
		}
		return std::move(state);
	}

	static void Function(ClientContext &, TableFunctionInput &data, DataChunk &output) {
		auto &bind_data = data.bind_data->Cast<BindData>();
		auto &state = data.global_state->Cast<GlobalState>();
		const idx_t row_count = 10;
		if (state.offset >= row_count) {
			output.SetChildCardinality(0);
			return;
		}
		const auto batch_count = MinValue<idx_t>(row_count - state.offset, output.GetCapacity());
		const auto output_count = state.projection_ids.empty() ? state.column_ids.size() : state.projection_ids.size();
		for (idx_t output_idx = 0; output_idx < output_count; output_idx++) {
			auto scan_idx = state.projection_ids.empty() ? output_idx : state.projection_ids[output_idx];
			auto &name = bind_data.names[state.column_ids[scan_idx]];
			if (name == "region") {
				auto writer = FlatVector::Writer<string_t>(output.data[output_idx], batch_count);
				for (idx_t row = 0; row < batch_count; row++) {
					writer.WriteValue(string_t((state.offset + row + 1) % 2 == 0 ? "b" : "a"));
				}
				continue;
			}
			auto writer = FlatVector::Writer<int64_t>(output.data[output_idx], batch_count);
			for (idx_t row = 0; row < batch_count; row++) {
				auto id = NumericCast<int64_t>(state.offset + row + 1);
				writer.WriteValue(name == "id" ? id : id * 10);
			}
		}
		state.offset += batch_count;
		output.SetChildCardinality(batch_count);
	}

	static bool SupportsPushdownType(const FunctionData &bind_data, idx_t column_idx) {
		return bind_data.Cast<BindData>().names[column_idx] != "region";
	}

	static void Register(Connection &con, const string &name, bool filter_prune, bool projection_pushdown) {
		con.BeginTransaction();
		TableFunction function(Identifier(name), {LogicalType::BOOLEAN}, Function, Bind, GlobalInit);
		function.projection_pushdown = projection_pushdown;
		function.filter_pushdown = true;
		function.filter_prune = filter_prune;
		function.supports_pushdown_type = SupportsPushdownType;
		CreateTableFunctionInfo info(function);
		Catalog::GetSystemCatalog(*con.context).CreateTableFunction(*con.context, info);
		con.Commit();
	}
};

void RequireColumns(Connection &con, const string &query, const vector<vector<Value>> &columns) {
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->ColumnCount() == columns.size());
	for (idx_t column_idx = 0; column_idx < columns.size(); column_idx++) {
		REQUIRE(CHECK_COLUMN(result, column_idx, columns[column_idx]));
	}
}

} // namespace

TEST_CASE("Filters a table function refuses keep the scan output as planned", "[tablefunction]") {
	DuckDB db(nullptr);
	Connection con(db);
	RegionFilterRefusingScan::Register(con, "refusing_scan", true, true);
	RegionFilterRefusingScan::Register(con, "refusing_scan_no_prune", false, true);
	RegionFilterRefusingScan::Register(con, "refusing_scan_no_projection", false, false);

	const vector<Value> ids {2, 4, 6, 8, 10};
	const vector<Value> totals {20, 40, 60, 80, 100};
	const vector<Value> regions {"b", "b", "b", "b", "b"};

	// filter column not selected, selected, and output reordered
	RequireColumns(con, "SELECT id, total FROM refusing_scan(false) WHERE region = 'b' ORDER BY id", {ids, totals});
	RequireColumns(con, "SELECT region, id FROM refusing_scan(false) WHERE region = 'b' ORDER BY id", {regions, ids});
	RequireColumns(con, "SELECT total, id FROM refusing_scan(false) WHERE region = 'b' ORDER BY id", {totals, ids});
	// the filter column completes a reordered projection of the scan's full width
	RequireColumns(con, "SELECT total FROM refusing_scan(true) WHERE region = 'b' ORDER BY total", {totals});
	RequireColumns(con, "SELECT id, total FROM refusing_scan_no_prune(false) WHERE region = 'b' ORDER BY id",
	               {ids, totals});
	RequireColumns(con, "SELECT total FROM refusing_scan_no_projection(false) WHERE region = 'b' ORDER BY total",
	               {totals});
	RequireColumns(con, "SELECT region, total FROM refusing_scan_no_projection(true) WHERE region = 'b' ORDER BY total",
	               {regions, totals});
	// a hash join builds from the filtered scan
	RequireColumns(con,
	               "SELECT f.total FROM refusing_scan(false) f JOIN refusing_scan(false) d ON f.id = d.id "
	               "WHERE d.region = 'b' ORDER BY f.total",
	               {totals});
}
