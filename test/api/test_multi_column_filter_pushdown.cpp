#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

using namespace duckdb;

namespace {

const column_t VIRTUAL_COLUMN_ID = VIRTUAL_COLUMN_START + 1;

//! Binds the test scan's columns: a, b and d are INTEGER, c is VARCHAR, e and f are DECIMAL(18, 3)
unique_ptr<FunctionData> MultiColumnScanBind(ClientContext &, TableFunctionBindInput &, vector<LogicalType> &types,
                                             vector<Identifier> &names) {
	types = {LogicalType::INTEGER, LogicalType::INTEGER,        LogicalType::VARCHAR,
	         LogicalType::INTEGER, LogicalType::DECIMAL(18, 3), LogicalType::DECIMAL(18, 3)};
	names = {"a", "b", "c", "d", "e", "f"};
	return make_uniq<TableFunctionData>();
}

//! Produces no rows; the tests only plan queries
void MultiColumnScanFunction(ClientContext &, TableFunctionInput &, DataChunk &) {
}

//! Takes every expression offered to the scan that cannot throw
bool MultiColumnScanPushdownExpression(ClientContext &, const LogicalGet &, Expression &expr) {
	return !expr.CanThrow();
}

//! Exposes one virtual INTEGER column, v
virtual_column_map_t MultiColumnScanVirtualColumns(ClientContext &, optional_ptr<FunctionData>) {
	virtual_column_map_t result;
	result.emplace(VIRTUAL_COLUMN_ID, TableColumn("v", LogicalType::INTEGER));
	return result;
}

//! The test scan, offered multi-column filters when multi_column_filter_pushdown is set
TableFunction MultiColumnScan(const char *name, bool multi_column_filter_pushdown) {
	TableFunction function(name, {}, MultiColumnScanFunction, MultiColumnScanBind);
	function.projection_pushdown = true;
	function.filter_pushdown = true;
	function.pushdown_expression = MultiColumnScanPushdownExpression;
	function.get_virtual_columns = MultiColumnScanVirtualColumns;
	function.multi_column_filter_pushdown = multi_column_filter_pushdown;
	return function;
}

//! Returns whether a and b are equal; the function is CONSISTENT_WITHIN_QUERY and cannot throw
void SameWithinQuery(DataChunk &args, ExpressionState &, Vector &result) {
	BinaryExecutor::Execute<int32_t, int32_t, bool>(args.data[0], args.data[1], result, args.size(),
	                                                [](int32_t a, int32_t b) { return a == b; });
}

//! An in-memory database with the test scans and the same_within_query function registered
struct ScanFixture {
	//! Registers offering_scan (flag set), plain_scan (flag not set) and same_within_query
	ScanFixture() : db(nullptr), con(db) {
		ExtensionLoader loader(*db.instance, "multi_column_filter_pushdown_test");
		loader.RegisterFunction(MultiColumnScan("offering_scan", true));
		loader.RegisterFunction(MultiColumnScan("plain_scan", false));
		ScalarFunction same_within_query("same_within_query", {LogicalType::INTEGER, LogicalType::INTEGER},
		                                 LogicalType::BOOLEAN, SameWithinQuery);
		same_within_query.SetStability(FunctionStability::CONSISTENT_WITHIN_QUERY);
		same_within_query.SetErrorMode(FunctionErrors::CANNOT_ERROR);
		loader.RegisterFunction(same_within_query);
	}

	DuckDB db;
	Connection con;
};

//! Searches the plan for a LogicalFilter that sits directly on a LogicalGet; returns null when there is none
optional_ptr<LogicalFilter> FindFilterOnGet(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_FILTER && op.children.size() == 1 &&
	    op.children[0]->type == LogicalOperatorType::LOGICAL_GET) {
		return op.Cast<LogicalFilter>();
	}
	for (auto &child : op.children) {
		auto result = FindFilterOnGet(*child);
		if (result) {
			return result;
		}
	}
	return nullptr;
}

//! A planned query with the filter on its scan and the scan itself
struct PlannedScan {
	string query;
	unique_ptr<LogicalOperator> plan;
	optional_ptr<LogicalFilter> filter;
	optional_ptr<LogicalGet> get;
};

//! Plans the query and requires a filter directly on a scan
PlannedScan PlanScan(Connection &con, const string &query) {
	CAPTURE(query);
	PlannedScan result;
	result.query = query;
	result.plan = con.ExtractPlan(query);
	result.filter = FindFilterOnGet(*result.plan);
	REQUIRE(result.filter);
	result.get = result.filter->children[0]->Cast<LogicalGet>();
	return result;
}

//! Requires the query's filter is kept above the scan and offered to it as one filter over two columns
void RequireOffered(const PlannedScan &scan) {
	CAPTURE(scan.query);
	REQUIRE(!scan.filter->expressions.empty());
	auto &multi_column_filters = scan.get->table_filters.GetMultiColumnFilters();
	REQUIRE(multi_column_filters.size() == 1);
	auto &filter = multi_column_filters[0]->Cast<ExpressionFilter>();
	REQUIRE(filter.column_indexes.size() == 2);
}

//! Requires the query's filter is kept above the scan and the scan receives no multi-column filter
void RequireNotOffered(const PlannedScan &scan) {
	CAPTURE(scan.query);
	REQUIRE(!scan.filter->expressions.empty());
	REQUIRE(scan.get->table_filters.GetMultiColumnFilters().empty());
}

} // namespace

TEST_CASE("A scan offering multi-column filters receives them as partial filters", "[api][multi_column_filter]") {
	ScanFixture fixture;
	auto &con = fixture.con;
	con.BeginTransaction();

	RequireOffered(PlanScan(con, "SELECT c FROM offering_scan() WHERE a = 1 OR b = 2"));
	RequireOffered(PlanScan(con, "SELECT c FROM offering_scan() WHERE a < b"));
	RequireOffered(PlanScan(con, "SELECT c FROM offering_scan() WHERE a IS NULL OR b = 2"));
	RequireOffered(PlanScan(con, "SELECT c FROM offering_scan() WHERE NOT (a = b)"));
	RequireOffered(PlanScan(con, "SELECT c FROM offering_scan() WHERE a IN (1, 2) OR b = 3"));

	// negating a DECIMAL cannot throw
	auto arithmetic = PlanScan(con, "SELECT c FROM offering_scan() WHERE -e < f");
	REQUIRE(arithmetic.filter->expressions.size() == 1);
	REQUIRE(!arithmetic.filter->expressions[0]->CanThrow());
	RequireOffered(arithmetic);

	con.Rollback();
}

TEST_CASE("A scan receives no multi-column filter it cannot take", "[api][multi_column_filter]") {
	ScanFixture fixture;
	auto &con = fixture.con;
	con.BeginTransaction();

	SECTION("volatile") {
		auto scan = PlanScan(con, "SELECT c FROM offering_scan() WHERE a < b + random()");
		REQUIRE(scan.filter->expressions.size() == 1);
		REQUIRE(scan.filter->expressions[0]->IsVolatile());
		RequireNotOffered(scan);
	}
	SECTION("the scan refuses it") {
		// consistent, so it is offered to pushdown_expression, which refuses it because it can throw
		auto scan = PlanScan(con, "SELECT c FROM offering_scan() WHERE a + b > 5");
		REQUIRE(scan.filter->expressions.size() == 1);
		REQUIRE(scan.filter->expressions[0]->CanThrow());
		REQUIRE(scan.filter->expressions[0]->IsConsistent());
		RequireNotOffered(scan);
	}
	SECTION("consistent only within the query") {
		auto scan = PlanScan(con, "SELECT c FROM offering_scan() WHERE same_within_query(a, b)");
		REQUIRE(scan.filter->expressions.size() == 1);
		auto &expr = *scan.filter->expressions[0];
		REQUIRE(!expr.IsVolatile());
		REQUIRE(!expr.CanThrow());
		REQUIRE(!expr.IsConsistent());
		RequireNotOffered(scan);
	}
	SECTION("virtual column") {
		RequireNotOffered(PlanScan(con, "SELECT c FROM offering_scan() WHERE a < v"));
	}
	SECTION("flag off") {
		RequireNotOffered(PlanScan(con, "SELECT c FROM plain_scan() WHERE a = 1 OR b = 2"));
		RequireNotOffered(PlanScan(con, "SELECT c FROM plain_scan() WHERE a < b"));
	}

	con.Rollback();
}

TEST_CASE("A multi-column filter references the scan columns it was written over after unused columns are removed",
          "[api][multi_column_filter]") {
	ScanFixture fixture;
	auto &con = fixture.con;
	con.BeginTransaction();

	// d comes first in the scan's columns and is unused, so removing it shifts the positions of a and b
	auto scan = PlanScan(con, "SELECT c FROM (SELECT d, c, a, b FROM offering_scan()) WHERE a = 1 OR b = 2");
	auto &column_ids = scan.get->GetColumnIds();
	for (auto &column_id : column_ids) {
		REQUIRE(column_id.GetPrimaryIndex() != 3);
	}
	auto &multi_column_filters = scan.get->table_filters.GetMultiColumnFilters();
	REQUIRE(multi_column_filters.size() == 1);
	auto &filter = multi_column_filters[0]->Cast<ExpressionFilter>();
	REQUIRE(filter.column_indexes.size() == 2);

	// each reference compared with a constant resolves to the column the query compared with that constant
	map<int32_t, idx_t> column_of_constant;
	// records which scan column each constant in an equality is compared with
	ExpressionIterator::EnumerateExpression(filter.expr, [&](Expression &expr) {
		if (expr.GetExpressionType() != ExpressionType::COMPARE_EQUAL) {
			return;
		}
		optional_ptr<BoundReferenceExpression> reference;
		optional_ptr<BoundConstantExpression> constant;
		// picks out the equality's column reference and constant
		ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) {
			if (child.GetExpressionClass() == ExpressionClass::BOUND_REF) {
				reference = child.Cast<BoundReferenceExpression>();
			} else if (child.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
				constant = child.Cast<BoundConstantExpression>();
			}
		});
		REQUIRE(reference);
		REQUIRE(constant);
		REQUIRE(reference->Index() < filter.column_indexes.size());
		auto projection_index = filter.column_indexes[reference->Index()].GetIndex();
		REQUIRE(projection_index < column_ids.size());
		column_of_constant[constant->GetValue().GetValue<int32_t>()] = column_ids[projection_index].GetPrimaryIndex();
	});
	REQUIRE(column_of_constant.size() == 2);
	REQUIRE(column_of_constant[1] == 0);
	REQUIRE(column_of_constant[2] == 1);

	con.Rollback();
}
