#include "catch.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

namespace {

struct BindData : public TableFunctionData {
	mutable string alias;
	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<BindData>();
		result->alias = alias;
		return std::move(result);
	}
};

void PushdownComplexFilter(ClientContext &, LogicalGet &get, FunctionData *bind_data_p,
                           vector<unique_ptr<Expression>> &filters) {
	auto &bind_data = bind_data_p->Cast<BindData>();
	for (auto &expr : filters) {
		ExpressionIterator::VisitExpressionMutable<BoundColumnRefExpression>(expr, [&](auto &ref, auto &) {
			if (ref.Binding().table_index == get.table_index && bind_data.alias.empty()) {
				bind_data.alias = ref.GetAlias().GetIdentifierName();
			}
		});
	}
}

void RegisterAliasInspect(DuckDB &db) {
	auto function = [](auto &, auto &, auto &output) {
		output.SetChildCardinality(0);
	};
	auto init = [](auto &, auto &) -> unique_ptr<GlobalTableFunctionState> {
		return {};
	};
	auto bind = [](auto &, auto &, auto &types, auto &names) -> unique_ptr<FunctionData> {
		types.emplace_back(LogicalType::INTEGER);
		names.emplace_back("col");
		return make_uniq<BindData>();
	};

	TableFunction tf("alias_inspect", {}, function, bind, init);

	tf.pushdown_complex_filter = PushdownComplexFilter;
	tf.to_string = [](auto &input) {
		InsertionOrderPreservingMap<string> result;
		if (auto bind_data = input.bind_data.get(); bind_data) {
			auto &data = bind_data->template Cast<BindData>();
			result["FilterAlias"] = data.alias.empty() ? "<none>" : data.alias;
		}
		return result;
	};

	ExtensionInfo extension_info {};
	ExtensionActiveLoad load_info {*db.instance, extension_info, "test_extension", ""};
	ExtensionLoader loader {load_info};
	loader.RegisterFunction(tf);
}

string GetExplain(Connection &con, const string &query) {
	auto result = con.Query("EXPLAIN " + query);
	REQUIRE_NO_FAIL(*result);
	string explain;
	for (idx_t row = 0; row < result->RowCount(); row++) {
		for (idx_t col = 0; col < result->ColumnCount(); col++) {
			explain += result->GetValue(col, row).ToString();
			explain += "\n";
		}
	}
	return explain;
}

} // namespace

TEST_CASE("Filter pushdown normalizes BoundColumnRef alias to scan column name", "[optimizer][filter_pushdown]") {
	DuckDB db(nullptr);
	Connection con(db);
	RegisterAliasInspect(db);

	REQUIRE_NO_FAIL(con.Query("PRAGMA explain_output='optimized_only'"));

	SECTION("subquery with column rename") {
		const auto explain =
		    GetExplain(con, "SELECT * FROM (SELECT col AS other FROM alias_inspect()) sub WHERE other IS NOT NULL");
		REQUIRE(StringUtil::Contains(explain, "FilterAlias: col"));
		REQUIRE_FALSE(StringUtil::Contains(explain, "FilterAlias: other"));
	}

	SECTION("CTE with column rename") {
		const auto explain = GetExplain(con, "WITH cte AS (SELECT col AS other FROM alias_inspect()) "
		                                     "SELECT * FROM cte WHERE other IS NOT NULL ORDER BY other");
		REQUIRE(StringUtil::Contains(explain, "FilterAlias: col"));
		REQUIRE_FALSE(StringUtil::Contains(explain, "FilterAlias: other"));
	}
}
