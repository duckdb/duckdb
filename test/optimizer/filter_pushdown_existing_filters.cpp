#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/common/in_clause.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/expression_barrier.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

using namespace duckdb;

namespace {

struct RepeatedPushdownData : TableFunctionData {
	vector<idx_t> callback_filter_counts;
	mutable vector<unique_ptr<Expression>> generic_filters;
};

unique_ptr<LogicalGet> MakeRepeatedPushdownScan(const LogicalType &type = LogicalType::INTEGER) {
	TableFunction function;
	function.filter_pushdown = true;
	function.pushdown_expression = [](auto &, auto &, auto &) {
		return true;
	};
	function.pushdown_complex_filter = [](auto &, auto &get, auto, auto &filters) {
		get.bind_data.write().template Cast<RepeatedPushdownData>().callback_filter_counts.push_back(filters.size());
	};
	auto get =
	    make_uniq<LogicalGet>(TableIndex(42), BoundTableFunction(function), make_uniq<RepeatedPushdownData>(),
	                          vector<LogicalType> {LogicalType::VARCHAR, type}, vector<Identifier> {"unused", "i"});
	get->AddColumnId(1);
	return get;
}

unique_ptr<Expression> RepeatedPushdownColumn(const LogicalType &type = LogicalType::INTEGER) {
	return make_uniq<BoundColumnRefExpression>(type, ColumnBinding(TableIndex(42), ProjectionIndex(0)));
}

unique_ptr<LogicalOperator> PushRepeatedScanFilter(Optimizer &optimizer, unique_ptr<LogicalOperator> plan,
                                                   unique_ptr<Expression> expression) {
	auto filter = make_uniq<LogicalFilter>(std::move(expression));
	filter->children.push_back(std::move(plan));
	FilterPushdown pushdown(optimizer);
	return pushdown.Rewrite(std::move(filter));
}

} // namespace

TEST_CASE("Scan filters normalize only after the complete batch is available", "[optimizer][filter_pushdown]") {
	const auto barrier = GENERATE(false, true);
	TableFilterSet filters;
	for (int32_t value : {1, 10}) {
		filters.PushFilter(ProjectionIndex(0), ExpressionFilter::CreateComparisonFilter(
		                                           ExpressionType::COMPARE_GREATERTHAN, Value::INTEGER(value)));
	}
	auto &expr = *filters.GetFilterByColumnIndex(ProjectionIndex(0)).Cast<ExpressionFilter>().expr;
	REQUIRE(expr.Cast<BoundConjunctionExpression>().GetChildren().size() == 2);
	if (barrier) {
		auto upper = ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_LESSTHAN, Value::INTEGER(20));
		filters.PushFilter(ProjectionIndex(0),
		                   make_uniq<ExpressionFilter>(ExpressionBarrier::Wrap(std::move(upper->expr))));
	}
	auto original = filters.Copy();
	REQUIRE(filters.Finalize() == FilterResult::SUCCESS);
	if (barrier) {
		REQUIRE(filters.Equals(*original));
	} else {
		auto expected =
		    ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_GREATERTHAN, Value::INTEGER(10));
		REQUIRE(
		    filters.GetFilterByColumnIndex(ProjectionIndex(0)).Cast<ExpressionFilter>().expr->Equals(*expected->expr));
	}
	auto finalized = filters.Copy();
	REQUIRE(filters.Finalize() == FilterResult::SUCCESS);
	REQUIRE(filters.Equals(*finalized));
}

TEST_CASE("Scan comparisons inherit required evaluation when an optional bound wins", "[optimizer][filter_pushdown]") {
	const auto reverse = GENERATE(false, true);
	const auto adaptive = GENERATE(false, true);
	const auto upper = GENERATE(false, true);
	const auto optional_stronger = GENERATE(false, true);
	const auto comparison = upper ? ExpressionType::COMPARE_LESSTHAN : ExpressionType::COMPARE_GREATERTHAN;
	auto bound = [&](int32_t value) {
		return ExpressionFilter::CreateComparisonFilter(comparison, Value::INTEGER(upper ? -value : value));
	};
	auto optional = bound(optional_stronger ? 20 : 5);
	optional->expr =
	    adaptive ? CreateSelectivityOptionalFilterExpression(std::move(optional->expr), LogicalType::INTEGER, 0.0f, 8)
	             : CreateOptionalFilterExpression(std::move(optional->expr), LogicalType::INTEGER);
	auto mandatory = bound(10);
	TableFilterSet filters;
	filters.PushFilter(ProjectionIndex(0), reverse ? std::move(optional) : std::move(mandatory));
	filters.PushFilter(ProjectionIndex(0), reverse ? std::move(mandatory) : std::move(optional));
	REQUIRE(filters.Finalize() == FilterResult::SUCCESS);
	auto expected = bound(optional_stronger ? 20 : 10);
	REQUIRE(filters.GetFilterByColumnIndex(ProjectionIndex(0)).Cast<ExpressionFilter>().expr->Equals(*expected->expr));

	// A second stronger optional bound must inherit enforcement from the first promoted bound.
	auto strongest = bound(30);
	strongest->expr = CreateOptionalFilterExpression(std::move(strongest->expr), LogicalType::INTEGER);
	filters.PushFilter(ProjectionIndex(0), std::move(strongest));
	REQUIRE(filters.Finalize() == FilterResult::SUCCESS);
	expected = bound(30);
	REQUIRE(filters.GetFilterByColumnIndex(ProjectionIndex(0)).Cast<ExpressionFilter>().expr->Equals(*expected->expr));
}

TEST_CASE("Optional-only bounds and contradictory scan entries keep their original form",
          "[optimizer][filter_pushdown]") {
	TableFilterSet filters;
	for (int32_t value : {10, 20, 30}) {
		auto bound =
		    ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_GREATERTHAN, Value::INTEGER(value));
		bound->expr = CreateOptionalFilterExpression(std::move(bound->expr), LogicalType::INTEGER);
		filters.PushFilter(ProjectionIndex(0), std::move(bound));
	}
	auto original = filters.Copy();
	REQUIRE(filters.Finalize() == FilterResult::SUCCESS);
	REQUIRE(filters.Equals(*original));
	filters.PushFilter(ProjectionIndex(0),
	                   ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_LESSTHAN, Value::INTEGER(5)));
	original = filters.Copy();
	REQUIRE(filters.Finalize() == FilterResult::UNSATISFIABLE);
	REQUIRE(filters.Equals(*original));
}

TEST_CASE("Exact scan IN filters replace only the comparisons they imply", "[optimizer][filter_pushdown]") {
	const auto mode = GENERATE(0, 1, 2);
	const auto reverse = GENERATE(false, true);
	const auto generic_sibling = GENERATE(false, true);
	const auto comparison = GENERATE(ExpressionType::COMPARE_EQUAL, ExpressionType::COMPARE_NOTEQUAL,
	                                 ExpressionType::COMPARE_GREATERTHAN, ExpressionType::COMPARE_GREATERTHANOREQUALTO,
	                                 ExpressionType::COMPARE_LESSTHAN, ExpressionType::COMPARE_LESSTHANOREQUALTO);
	const auto implied = comparison == ExpressionType::COMPARE_NOTEQUAL ||
	                     comparison == ExpressionType::COMPARE_GREATERTHANOREQUALTO ||
	                     comparison == ExpressionType::COMPARE_LESSTHANOREQUALTO;
	const auto bound =
	    comparison == ExpressionType::COMPARE_NOTEQUAL
	        ? 20
	        : (comparison == ExpressionType::COMPARE_LESSTHAN || comparison == ExpressionType::COMPARE_LESSTHANOREQUALTO
	               ? 30
	               : 10);
	auto in = ExpressionFilter::CreateInExpression(
	    make_uniq<BoundReferenceExpression>(LogicalType::INTEGER, 0ULL),
	    {Value::INTEGER(10), Value::INTEGER(30), Value(LogicalType::INTEGER), Value::INTEGER(10)});
	auto expected = in->Copy();
	if (mode == 1) {
		in = CreateOptionalFilterExpression(std::move(in), LogicalType::INTEGER);
	} else if (mode == 2) {
		in = CreateSelectivityOptionalFilterExpression(std::move(in), LogicalType::INTEGER, 0.0f, 8);
	}
	unique_ptr<TableFilter> membership = make_uniq<ExpressionFilter>(std::move(in));
	unique_ptr<TableFilter> required = ExpressionFilter::CreateComparisonFilter(comparison, Value::INTEGER(bound));
	TableFilterSet filters;
	filters.PushFilter(ProjectionIndex(0), reverse ? std::move(membership) : std::move(required));
	filters.PushFilter(ProjectionIndex(0), reverse ? std::move(required) : std::move(membership));
	if (generic_sibling) {
		auto sibling = ExpressionFilter::CreateNullCheckExpression(
		    make_uniq<BoundReferenceExpression>(LogicalType::INTEGER, 0ULL), ExpressionType::OPERATOR_IS_NOT_NULL);
		expected = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(expected),
		                                                 sibling->Copy());
		filters.PushFilter(ProjectionIndex(0), make_uniq<ExpressionFilter>(std::move(sibling)));
	}
	auto original = filters.Copy();
	REQUIRE(filters.Finalize() == FilterResult::SUCCESS);
	if (implied && mode == 0) {
		REQUIRE(filters.GetFilterByColumnIndex(ProjectionIndex(0)).Cast<ExpressionFilter>().expr->Equals(*expected));
	} else {
		REQUIRE(filters.Equals(*original));
	}
}

TEST_CASE("Scan IN implication requires a literal list on the same native column", "[optimizer][filter_pushdown]") {
	const auto shape = GENERATE(0, 1, 2, 3);
	auto in = ExpressionFilter::CreateInExpression(make_uniq<BoundReferenceExpression>(LogicalType::INTEGER, 0ULL),
	                                               {Value::INTEGER(20), Value::INTEGER(30)});
	auto &children = in->Cast<BoundOperatorExpression>().GetChildrenMutable();
	if (shape == 0) {
		children[1] = make_uniq<BoundReferenceExpression>(LogicalType::INTEGER, 0ULL);
	} else if (shape == 1) {
		children[1] = make_uniq<BoundConstantExpression>(Value::BIGINT(20));
	} else if (shape == 2) {
		children[0] = make_uniq<BoundReferenceExpression>(LogicalType::INTEGER, 1ULL);
	} else {
		DuckDB db;
		Connection con(db);
		children[0] = BoundCastExpression::AddCastToType(*con.context, std::move(children[0]), LogicalType::BIGINT);
	}
	TableFilterSet filters;
	filters.PushFilter(ProjectionIndex(0), make_uniq<ExpressionFilter>(std::move(in)));
	filters.PushFilter(ProjectionIndex(0), ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_GREATERTHAN,
	                                                                                Value::INTEGER(10)));
	auto original = filters.Copy();
	REQUIRE(filters.Finalize() == FilterResult::SUCCESS);
	REQUIRE(filters.Equals(*original));
}

TEST_CASE("Native interval IN subsumes bounds using normalized interval ordering", "[optimizer][filter_pushdown]") {
	auto in = ExpressionFilter::CreateInExpression(
	    make_uniq<BoundReferenceExpression>(LogicalType::INTERVAL, 0ULL),
	    {Value::INTERVAL(1, 0, 0), Value::INTERVAL(0, 31, 0), Value(LogicalType::INTERVAL)});
	auto expected = in->Copy();
	TableFilterSet filters;
	filters.PushFilter(ProjectionIndex(0), make_uniq<ExpressionFilter>(std::move(in)));
	filters.PushFilter(ProjectionIndex(0),
	                   ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_GREATERTHANOREQUALTO,
	                                                            Value::INTERVAL(0, 30, 0)));
	REQUIRE(filters.Finalize() == FilterResult::SUCCESS);
	REQUIRE(filters.GetFilterByColumnIndex(ProjectionIndex(0)).Cast<ExpressionFilter>().expr->Equals(*expected));
}

TEST_CASE("Scan IN subsumption uses the ordinary IN row-evaluation cutoff", "[optimizer][filter_pushdown]") {
	const auto mode = GENERATE(0, 1, 2);
	const auto value_count = GENERATE(IN_CLAUSE_REWRITE_THRESHOLD - 2, IN_CLAUSE_REWRITE_THRESHOLD - 1, idx_t(37));
	vector<Value> values;
	for (idx_t i = 0; i < value_count; i++) {
		values.push_back(Value::INTEGER(20 + NumericCast<int32_t>(i)));
	}
	auto in = ExpressionFilter::CreateInExpression(make_uniq<BoundReferenceExpression>(LogicalType::INTEGER, 0ULL),
	                                               std::move(values));
	auto expected = in->Copy();
	if (mode == 1) {
		in = CreateOptionalFilterExpression(std::move(in), LogicalType::INTEGER);
	} else if (mode == 2) {
		in = CreateSelectivityOptionalFilterExpression(std::move(in), LogicalType::INTEGER, 0.0f, 8);
	}
	TableFilterSet filters;
	filters.PushFilter(ProjectionIndex(0), make_uniq<ExpressionFilter>(std::move(in)));
	filters.PushFilter(ProjectionIndex(0), ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_GREATERTHAN,
	                                                                                Value::INTEGER(10)));
	filters.PushFilter(ProjectionIndex(0),
	                   ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_LESSTHAN, Value::INTEGER(100)));
	auto original = filters.Copy();
	REQUIRE(filters.Finalize() == FilterResult::SUCCESS);
	if (mode == 0 && value_count + 1 < IN_CLAUSE_REWRITE_THRESHOLD) {
		REQUIRE(filters.GetFilterByColumnIndex(ProjectionIndex(0)).Cast<ExpressionFilter>().expr->Equals(*expected));
	} else {
		REQUIRE(filters.Equals(*original));
	}
}

TEST_CASE("Optional scan IN remains optional without a required bound", "[optimizer][filter_pushdown]") {
	TableFilterSet filters;
	auto in = ExpressionFilter::CreateInExpression(make_uniq<BoundReferenceExpression>(LogicalType::INTEGER, 0ULL),
	                                               {Value::INTEGER(20), Value::INTEGER(30)});
	filters.PushFilter(ProjectionIndex(0), make_uniq<ExpressionFilter>(
	                                           CreateOptionalFilterExpression(std::move(in), LogicalType::INTEGER)));
	auto bound = ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_GREATERTHAN, Value::INTEGER(10));
	bound->expr = CreateOptionalFilterExpression(std::move(bound->expr), LogicalType::INTEGER);
	filters.PushFilter(ProjectionIndex(0), std::move(bound));
	auto original = filters.Copy();
	REQUIRE(filters.Finalize() == FilterResult::SUCCESS);
	REQUIRE(filters.Equals(*original));
}

TEST_CASE("Repeated scan pushdown separates mandatory filters from optional and runtime filters",
          "[optimizer][filter_pushdown]") {
	DuckDB db;
	Connection con(db);
	auto binder = Binder::CreateBinder(*con.context);
	Optimizer optimizer(*binder, *con.context);
	auto get = MakeRepeatedPushdownScan();
	auto &scan = *get;
	auto static_hint = ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_LESSTHAN, Value::INTEGER(50));
	for (float threshold : {0.5f, 0.5f, 0.8f}) {
		scan.table_filters.PushFilter(ProjectionIndex(0),
		                              make_uniq<ExpressionFilter>(CreateSelectivityOptionalFilterExpression(
		                                  static_hint->expr->Copy(), LogicalType::INTEGER, threshold, 8)));
	}
	auto dynamic = make_shared_ptr<DynamicFilterData>(ExpressionType::COMPARE_EQUAL, Value::INTEGER(0));
	reference_map_t<const Expression, idx_t> runtime_expressions;
	for (idx_t i = 0; i < 2; i++) {
		auto wrapper = CreateOptionalFilterExpression(CreateDynamicFilterExpression(dynamic, LogicalType::INTEGER),
		                                              LogicalType::INTEGER);
		runtime_expressions.emplace(*wrapper, runtime_expressions.size());
		scan.table_filters.PushFilter(ProjectionIndex(0), make_uniq<ExpressionFilter>(std::move(wrapper)));
		auto nested = BoundComparisonExpression::Create(
		    ExpressionType::COMPARE_EQUAL,
		    CreateOptionalFilterExpression(CreateDynamicFilterExpression(dynamic, LogicalType::INTEGER),
		                                   LogicalType::INTEGER),
		    make_uniq<BoundConstantExpression>(Value::BOOLEAN(true)));
		runtime_expressions.emplace(*nested, runtime_expressions.size());
		scan.table_filters.PushFilter(ProjectionIndex(0), make_uniq<ExpressionFilter>(std::move(nested)));
	}
	scan.table_filters.PushFilter(ProjectionIndex(0), ExpressionFilter::CreateComparisonFilter(
	                                                      ExpressionType::COMPARE_GREATERTHAN, Value::INTEGER(1)));
	unique_ptr<LogicalOperator> plan = std::move(get);
	for (idx_t i = 0; i < 3; i++) {
		plan = PushRepeatedScanFilter(
		    optimizer, std::move(plan),
		    BoundComparisonExpression::Create(ExpressionType::COMPARE_GREATERTHAN, RepeatedPushdownColumn(),
		                                      make_uniq<BoundConstantExpression>(Value::INTEGER(10))));
		REQUIRE(plan->type == LogicalOperatorType::LOGICAL_GET);
		vector<unique_ptr<Expression>> conjuncts;
		conjuncts.push_back(
		    scan.table_filters.GetFilterByColumnIndex(ProjectionIndex(0)).Cast<ExpressionFilter>().expr->Copy());
		LogicalFilter::SplitPredicates(conjuncts);
		// Two static configurations, four runtime wrappers (including nested ones), and the stronger predicate.
		REQUIRE(conjuncts.size() == 7);
		idx_t runtime_count = 0;
		idx_t mandatory_count = 0;
		auto expected =
		    ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_GREATERTHAN, Value::INTEGER(10));
		for (auto &expr : conjuncts) {
			mandatory_count += expr->Equals(*expected->expr);
			if (!ExpressionFilter::ContainsInternalFunction(*expr, DynamicFilterScalarFun::NAME)) {
				continue;
			}
			optional_ptr<const Expression> wrapper = *expr;
			if (expr->GetExpressionType() == ExpressionType::COMPARE_EQUAL) {
				wrapper = BoundComparisonExpression::Left(expr->Cast<BoundFunctionExpression>());
			}
			auto child = ExpressionFilter::GetOptionalFilterChild(*wrapper);
			REQUIRE(child);
			REQUIRE(child->Cast<BoundFunctionExpression>().BindInfo()->Cast<DynamicFilterFunctionData>().filter_data ==
			        dynamic);
			runtime_count++;
		}
		REQUIRE(mandatory_count == 1);
		REQUIRE(runtime_count == 4);
		idx_t surviving_runtime_count = 0;
		auto &scan_expr = scan.table_filters.GetFilterByColumnIndex(ProjectionIndex(0)).Cast<ExpressionFilter>().expr;
		ExpressionIterator::VisitExpression<BoundFunctionExpression>(
		    *scan_expr, [&](const BoundFunctionExpression &expr) {
			    auto entry = runtime_expressions.find(expr);
			    if (entry != runtime_expressions.end()) {
				    REQUIRE(entry->second == surviving_runtime_count++);
			    }
		    });
		REQUIRE(surviving_runtime_count == runtime_expressions.size());
	}
	auto &counts = scan.bind_data->Cast<RepeatedPushdownData>().callback_filter_counts;
	REQUIRE(counts == vector<idx_t> {1, 1, 1});
}

TEST_CASE("Repeated partial IN pushdown retains one scan predicate and its residual", "[optimizer][filter_pushdown]") {
	const auto required_bound = GENERATE(false, true);
	DuckDB db;
	Connection con(db);
	auto binder = Binder::CreateBinder(*con.context);
	Optimizer optimizer(*binder, *con.context);
	auto get = MakeRepeatedPushdownScan();
	auto &scan = *get;
	if (required_bound) {
		scan.table_filters.PushFilter(ProjectionIndex(0), ExpressionFilter::CreateComparisonFilter(
		                                                      ExpressionType::COMPARE_GREATERTHAN, Value::INTEGER(0)));
	}
	auto in = make_uniq<BoundOperatorExpression>(ExpressionType::COMPARE_IN, LogicalType::BOOLEAN);
	in->GetChildrenMutable().push_back(RepeatedPushdownColumn());
	for (int32_t value : {1, 3, 5}) {
		in->GetChildrenMutable().push_back(make_uniq<BoundConstantExpression>(Value::INTEGER(value)));
	}
	auto filter = make_uniq<LogicalFilter>(std::move(in));
	filter->children.push_back(std::move(get));
	unique_ptr<LogicalOperator> plan = std::move(filter);
	unique_ptr<TableFilterSet> first_filters;
	for (idx_t i = 0; i < 3; i++) {
		FilterPushdown pushdown(optimizer);
		plan = pushdown.Rewrite(std::move(plan));
		REQUIRE(plan->type == LogicalOperatorType::LOGICAL_FILTER);
		REQUIRE(plan->expressions.size() == 1);
		REQUIRE(scan.table_filters.FilterCount() == 1);
		auto &hint = scan.table_filters.GetFilterByColumnIndex(ProjectionIndex(0));
		REQUIRE(ExpressionFilter::IsRootOptionalFilter(hint) == !required_bound);
		if (first_filters) {
			REQUIRE(scan.table_filters.Equals(*first_filters));
		} else {
			first_filters = scan.table_filters.Copy();
		}
	}
}

TEST_CASE("Repeated scan pushdown keeps ranges as comparisons", "[optimizer][filter_pushdown]") {
	DuckDB db;
	Connection con(db);
	auto binder = Binder::CreateBinder(*con.context);
	Optimizer optimizer(*binder, *con.context);
	auto get = MakeRepeatedPushdownScan();
	auto &scan = *get;
	// A scan can support comparison pushdown without accepting generic expressions such as BETWEEN.
	scan.function.pushdown_expression = nullptr;
	auto lower = ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_GREATERTHAN, Value::INTEGER(10));
	auto expected_lower = lower->expr->Copy();
	scan.table_filters.PushFilter(ProjectionIndex(0), std::move(lower));
	auto plan = PushRepeatedScanFilter(
	    optimizer, std::move(get),
	    BoundComparisonExpression::Create(ExpressionType::COMPARE_LESSTHAN, RepeatedPushdownColumn(),
	                                      make_uniq<BoundConstantExpression>(Value::INTEGER(20))));
	REQUIRE(plan->type == LogicalOperatorType::LOGICAL_GET);
	vector<unique_ptr<Expression>> conjuncts;
	conjuncts.push_back(
	    scan.table_filters.GetFilterByColumnIndex(ProjectionIndex(0)).Cast<ExpressionFilter>().expr->Copy());
	LogicalFilter::SplitPredicates(conjuncts);
	REQUIRE(conjuncts.size() == 2);
	auto upper = ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_LESSTHAN, Value::INTEGER(20));
	REQUIRE(((conjuncts[0]->Equals(*expected_lower) && conjuncts[1]->Equals(*upper->expr)) ||
	         (conjuncts[1]->Equals(*expected_lower) && conjuncts[0]->Equals(*upper->expr))));
	plan = PushRepeatedScanFilter(
	    optimizer, std::move(plan),
	    BoundComparisonExpression::Create(ExpressionType::COMPARE_LESSTHANOREQUALTO, RepeatedPushdownColumn(),
	                                      make_uniq<BoundConstantExpression>(Value::INTEGER(10))));
	REQUIRE(plan->type == LogicalOperatorType::LOGICAL_EMPTY_RESULT);
}

TEST_CASE("Repeated multi-column pushdown retains projection mappings and its residual",
          "[optimizer][filter_pushdown]") {
	DuckDB db;
	Connection con(db);
	REQUIRE_FALSE(con.Query("CREATE TABLE multi (a INTEGER, b INTEGER)")->HasError());
	con.BeginTransaction();
	auto plan = TestExtractPlan(con, "SELECT b, a FROM multi");
	REQUIRE(plan->children.size() == 1);
	auto child = std::move(plan->children[0]);
	plan = std::move(child);
	REQUIRE(plan->type == LogicalOperatorType::LOGICAL_GET);
	auto &scan = plan->Cast<LogicalGet>();
	REQUIRE(scan.GetColumnIds()[ProjectionIndex(0)].GetPrimaryIndex() == 1);
	REQUIRE(scan.GetColumnIds()[ProjectionIndex(1)].GetPrimaryIndex() == 0);
	scan.table_filters.PushFilter(ProjectionIndex(0), ExpressionFilter::CreateComparisonFilter(
	                                                      ExpressionType::COMPARE_GREATERTHAN, Value::INTEGER(1)));
	auto comparison = BoundComparisonExpression::Create(
	    ExpressionType::COMPARE_LESSTHAN,
	    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, ColumnBinding(scan.table_index, ProjectionIndex(1))),
	    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, ColumnBinding(scan.table_index, ProjectionIndex(0))));
	auto expected_residual = comparison->Copy();
	auto binder = Binder::CreateBinder(*con.context);
	Optimizer optimizer(*binder, *con.context);
	plan = PushRepeatedScanFilter(optimizer, std::move(plan), std::move(comparison));
	for (idx_t i = 0; i < 3; i++) {
		REQUIRE(plan->type == LogicalOperatorType::LOGICAL_FILTER);
		REQUIRE(plan->expressions.size() == 1);
		REQUIRE(plan->expressions[0]->Equals(*expected_residual));
		auto &multi = scan.table_filters.GetMultiColumnFilters();
		REQUIRE(multi.size() == 1);
		auto &filter = multi[0]->Cast<ExpressionFilter>();
		REQUIRE(filter.column_indexes == vector<ProjectionIndex> {ProjectionIndex(1), ProjectionIndex(0)});
		auto &expr = filter.expr->Cast<BoundFunctionExpression>();
		REQUIRE(BoundComparisonExpression::Left(expr).Cast<BoundReferenceExpression>().Index() == 0);
		REQUIRE(BoundComparisonExpression::Right(expr).Cast<BoundReferenceExpression>().Index() == 1);
		FilterPushdown pushdown(optimizer);
		plan = pushdown.Rewrite(std::move(plan));
	}
}

TEST_CASE("Repeated scan pushdown preserves admitted generic expressions", "[optimizer][filter_pushdown]") {
	DuckDB db;
	Connection con(db);
	con.BeginTransaction();
	auto binder = Binder::CreateBinder(*con.context);
	Optimizer optimizer(*binder, *con.context);
	auto get = MakeRepeatedPushdownScan(LogicalType::VARCHAR);
	auto &scan = *get;
	scan.function.pushdown_expression = [](auto &, auto &get, auto &expr) {
		auto &data = get.bind_data->template Cast<RepeatedPushdownData>();
		data.generic_filters.push_back(expr.Copy());
		return expr.GetExpressionType() == ExpressionType::COMPARE_BETWEEN;
	};
	auto length = optimizer.BindScalarFunction("length", RepeatedPushdownColumn(LogicalType::VARCHAR));
	auto between = BoundBetweenExpression::Create(*con.context, length->Copy(),
	                                              make_uniq<BoundConstantExpression>(Value::BIGINT(2)),
	                                              make_uniq<BoundConstantExpression>(Value::BIGINT(8)), true, true);
	REQUIRE_FALSE(between->CanThrow());
	auto plan = PushRepeatedScanFilter(optimizer, std::move(get), std::move(between));
	REQUIRE(plan->type == LogicalOperatorType::LOGICAL_GET);
	auto &admitted = scan.table_filters.GetFilterByColumnIndex(ProjectionIndex(0)).Cast<ExpressionFilter>();
	REQUIRE(admitted.expr->GetExpressionType() == ExpressionType::COMPARE_BETWEEN);
	reference<const Expression> admitted_expression = *admitted.expr;
	auto admitted_copy = admitted.expr->Copy();

	scan.table_filters.PushFilter(ProjectionIndex(0), ExpressionFilter::CreateComparisonFilter(
	                                                      ExpressionType::COMPARE_GREATERTHANOREQUALTO, Value("a")));
	// Removing this weaker native bound must preserve the admitted BETWEEN and its position.
	plan = PushRepeatedScanFilter(optimizer, std::move(plan),
	                              BoundComparisonExpression::Create(ExpressionType::COMPARE_EQUAL,
	                                                                RepeatedPushdownColumn(LogicalType::VARCHAR),
	                                                                make_uniq<BoundConstantExpression>(Value("abc"))));
	REQUIRE(plan->type == LogicalOperatorType::LOGICAL_GET);
	auto &conjunction = scan.table_filters.GetFilterByColumnIndex(ProjectionIndex(0))
	                        .Cast<ExpressionFilter>()
	                        .expr->Cast<BoundConjunctionExpression>();
	REQUIRE(conjunction.GetChildren().size() == 2);
	REQUIRE(ReferenceEquality<const Expression>()(*conjunction.GetChildren()[0], admitted_expression));
	REQUIRE(conjunction.GetChildren()[0]->Equals(*admitted_copy));
	auto expected_equality = ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_EQUAL, Value("abc"));
	REQUIRE(conjunction.GetChildren()[1]->Equals(*expected_equality->expr));

	auto rejected = BoundComparisonExpression::Create(ExpressionType::COMPARE_GREATERTHAN, std::move(length),
	                                                  make_uniq<BoundConstantExpression>(Value::BIGINT(3)));
	auto rejected_copy = rejected->Copy();
	plan = PushRepeatedScanFilter(optimizer, std::move(plan), std::move(rejected));
	REQUIRE(plan->type == LogicalOperatorType::LOGICAL_FILTER);
	REQUIRE(plan->expressions.size() == 1);
	REQUIRE(plan->expressions[0]->Equals(*rejected_copy));
	auto &requests = scan.bind_data->Cast<RepeatedPushdownData>().generic_filters;
	REQUIRE(requests.size() == 2);
	REQUIRE(requests[0]->GetExpressionType() == ExpressionType::COMPARE_BETWEEN);
	REQUIRE(requests[1]->Equals(*rejected_copy));
	REQUIRE(scan.bind_data->Cast<RepeatedPushdownData>().callback_filter_counts == vector<idx_t> {1, 1, 1});
}

TEST_CASE("Repeated scan pushdown respects typed comparison semantics", "[optimizer][filter_pushdown]") {
	DuckDB db;
	Connection con(db);
	auto binder = Binder::CreateBinder(*con.context);
	Optimizer optimizer(*binder, *con.context);
	LogicalType type = LogicalType::INTERVAL;
	Value lower = Value::INTERVAL(1, 0, 0);
	Value stronger = Value::INTERVAL(0, 31, 0);
	idx_t expected_count = 1;
	SECTION("Intervals strengthen beyond the native constant-filter types") {
	}
	SECTION("Equal interval representations share normalized ordering") {
		stronger = Value::INTERVAL(0, 30, 0);
	}
	SECTION("Native VARIANT comparisons remain opaque to Value ordering") {
		type = LogicalType::VARIANT();
		lower = Value::INTEGER(-100).DefaultCastAs(type);
		stronger = Value::INTEGER(0).DefaultCastAs(type);
		expected_count = 2;
	}
	auto get = MakeRepeatedPushdownScan(type);
	auto &scan = *get;
	scan.table_filters.PushFilter(ProjectionIndex(0), ExpressionFilter::CreateComparisonFilter(
	                                                      ExpressionType::COMPARE_GREATERTHANOREQUALTO, lower));
	auto plan = PushRepeatedScanFilter(optimizer, std::move(get),
	                                   BoundComparisonExpression::Create(ExpressionType::COMPARE_GREATERTHANOREQUALTO,
	                                                                     RepeatedPushdownColumn(type),
	                                                                     make_uniq<BoundConstantExpression>(stronger)));
	REQUIRE(plan->type == LogicalOperatorType::LOGICAL_GET);
	vector<unique_ptr<Expression>> conjuncts;
	conjuncts.push_back(
	    scan.table_filters.GetFilterByColumnIndex(ProjectionIndex(0)).Cast<ExpressionFilter>().expr->Copy());
	LogicalFilter::SplitPredicates(conjuncts);
	REQUIRE(conjuncts.size() == expected_count);
	auto expected = ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_GREATERTHANOREQUALTO, stronger);
	REQUIRE(conjuncts.back()->Equals(*expected->expr));
	for (auto &expr : conjuncts) {
		REQUIRE_FALSE(ExpressionFilter::ContainsInternalTableFilterFunction(*expr));
	}
}

TEST_CASE("Repeated scan pushdown keeps distinct struct fields", "[optimizer][filter_pushdown]") {
	DuckDB db;
	Connection con(db);
	con.BeginTransaction();
	auto binder = Binder::CreateBinder(*con.context);
	Optimizer optimizer(*binder, *con.context);
	auto type = LogicalType::STRUCT({{"a", LogicalType::INTEGER}, {"b", LogicalType::INTEGER}});
	auto get = MakeRepeatedPushdownScan(type);
	auto &scan = *get;
	auto field = [&](const string &name, unique_ptr<Expression> column) {
		return optimizer.BindScalarFunction("struct_extract", std::move(column),
		                                    make_uniq<BoundConstantExpression>(Value(name)));
	};
	auto a = field("a", make_uniq<BoundReferenceExpression>(type, 0));
	auto b = field("b", make_uniq<BoundReferenceExpression>(type, 0));
	scan.table_filters.PushFilter(ProjectionIndex(0), make_uniq<ExpressionFilter>(BoundComparisonExpression::Create(
	                                                      ExpressionType::COMPARE_GREATERTHAN, a->Copy(),
	                                                      make_uniq<BoundConstantExpression>(Value::INTEGER(1)))));
	auto b_filter = BoundComparisonExpression::Create(ExpressionType::COMPARE_GREATERTHAN, std::move(b),
	                                                  make_uniq<BoundConstantExpression>(Value::INTEGER(1)));
	reference<const Expression> b_expression = *b_filter;
	scan.table_filters.PushFilter(ProjectionIndex(0), make_uniq<ExpressionFilter>(std::move(b_filter)));
	auto plan = PushRepeatedScanFilter(
	    optimizer, std::move(get),
	    BoundComparisonExpression::Create(ExpressionType::COMPARE_GREATERTHAN, field("a", RepeatedPushdownColumn(type)),
	                                      make_uniq<BoundConstantExpression>(Value::INTEGER(10))));
	REQUIRE(plan->type == LogicalOperatorType::LOGICAL_GET);
	auto &children = scan.table_filters.GetFilterByColumnIndex(ProjectionIndex(0))
	                     .Cast<ExpressionFilter>()
	                     .expr->Cast<BoundConjunctionExpression>()
	                     .GetChildren();
	REQUIRE(children.size() == 2);
	REQUIRE(ReferenceEquality<const Expression>()(*children[0], b_expression));
	auto expected = BoundComparisonExpression::Create(ExpressionType::COMPARE_GREATERTHAN, std::move(a),
	                                                  make_uniq<BoundConstantExpression>(Value::INTEGER(10)));
	REQUIRE(children[1]->Equals(*expected));
}

TEST_CASE("Repeated scan pushdown retains a minimal untouched entry", "[optimizer][filter_pushdown]") {
	DuckDB db;
	Connection con(db);
	auto binder = Binder::CreateBinder(*con.context);
	Optimizer optimizer(*binder, *con.context);
	auto get = MakeRepeatedPushdownScan();
	auto &scan = *get;
	scan.AddColumnId(0);
	scan.table_filters.PushFilter(ProjectionIndex(0), ExpressionFilter::CreateComparisonFilter(
	                                                      ExpressionType::COMPARE_GREATERTHAN, Value::INTEGER(1)));
	for (auto comparison : {ExpressionType::COMPARE_GREATERTHANOREQUALTO, ExpressionType::COMPARE_LESSTHANOREQUALTO,
	                        ExpressionType::COMPARE_NOTEQUAL}) {
		const string value = comparison == ExpressionType::COMPARE_GREATERTHANOREQUALTO ? "a"
		                     : comparison == ExpressionType::COMPARE_LESSTHANOREQUALTO  ? "z"
		                                                                                : "m";
		scan.table_filters.PushFilter(ProjectionIndex(1),
		                              ExpressionFilter::CreateComparisonFilter(comparison, Value(value)));
	}
	reference<const Expression> original =
	    *scan.table_filters.GetFilterByColumnIndex(ProjectionIndex(1)).Cast<ExpressionFilter>().expr;
	auto copy = original.get().Copy();
	auto plan = PushRepeatedScanFilter(
	    optimizer, std::move(get),
	    BoundComparisonExpression::Create(ExpressionType::COMPARE_GREATERTHAN, RepeatedPushdownColumn(),
	                                      make_uniq<BoundConstantExpression>(Value::INTEGER(10))));
	REQUIRE(plan->type == LogicalOperatorType::LOGICAL_GET);
	auto &untouched = scan.table_filters.GetFilterByColumnIndex(ProjectionIndex(1)).Cast<ExpressionFilter>().expr;
	REQUIRE(ReferenceEquality<const Expression>()(*untouched, original));
	REQUIRE(untouched->Equals(*copy));
}

TEST_CASE("Repeated scan pushdown preserves evaluation boundaries", "[optimizer][filter_pushdown]") {
	DuckDB db;
	Connection con(db);
	auto binder = Binder::CreateBinder(*con.context);
	Optimizer optimizer(*binder, *con.context);
	auto get = MakeRepeatedPushdownScan();
	auto &scan = *get;
	scan.table_filters.PushFilter(ProjectionIndex(0), ExpressionFilter::CreateComparisonFilter(
	                                                      ExpressionType::COMPARE_GREATERTHAN, Value::INTEGER(1)));
	auto initial_filters = scan.table_filters.Copy();
	auto incoming = BoundComparisonExpression::Create(ExpressionType::COMPARE_GREATERTHAN, RepeatedPushdownColumn(),
	                                                  make_uniq<BoundConstantExpression>(Value::INTEGER(10)));

	SECTION("A late throwing predicate stays above its scan guard") {
		incoming = BoundComparisonExpression::Create(
		    ExpressionType::COMPARE_EQUAL,
		    BoundCastExpression::AddCastToType(*con.context, RepeatedPushdownColumn(), LogicalType::TINYINT),
		    make_uniq<BoundConstantExpression>(Value::TINYINT(10)));
		REQUIRE(incoming->CanThrow());
		auto plan = PushRepeatedScanFilter(optimizer, std::move(get), std::move(incoming));
		REQUIRE(plan->type == LogicalOperatorType::LOGICAL_FILTER);
		REQUIRE(plan->expressions.size() == 1);
		REQUIRE(plan->expressions[0]->CanThrow());
		REQUIRE(scan.table_filters.Equals(*initial_filters));
	}

	SECTION("Existing barred predicates retain their representation") {
		auto comparison =
		    ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_LESSTHAN, Value::INTEGER(20));
		auto barred = ExpressionBarrier::Wrap(std::move(comparison->expr));
		SECTION("A direct barrier") {
		}
		SECTION("A barrier in optional bind data below a comparison") {
			barred = BoundComparisonExpression::Create(
			    ExpressionType::COMPARE_EQUAL, CreateOptionalFilterExpression(std::move(barred), LogicalType::INTEGER),
			    make_uniq<BoundConstantExpression>(Value::BOOLEAN(true)));
		}
		auto expected = barred->Copy();
		scan.table_filters.PushFilter(ProjectionIndex(0), make_uniq<ExpressionFilter>(std::move(barred)));
		auto plan = PushRepeatedScanFilter(optimizer, std::move(get), std::move(incoming));
		REQUIRE(plan->type == LogicalOperatorType::LOGICAL_GET);
		vector<unique_ptr<Expression>> conjuncts;
		conjuncts.push_back(
		    scan.table_filters.GetFilterByColumnIndex(ProjectionIndex(0)).Cast<ExpressionFilter>().expr->Copy());
		LogicalFilter::SplitPredicates(conjuncts);
		idx_t matching_barriers = 0;
		idx_t matching_guards = 0;
		auto guard = ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_GREATERTHAN, Value::INTEGER(1));
		for (auto &expr : conjuncts) {
			matching_barriers += expr->Equals(*expected);
			matching_guards += expr->Equals(*guard->expr);
		}
		REQUIRE(matching_barriers == 1);
		REQUIRE(matching_guards == 1);
	}

	SECTION("Ordinality blocks an otherwise supported pushdown") {
		scan.ordinality_idx = 1;
		auto plan = PushRepeatedScanFilter(optimizer, std::move(get), std::move(incoming));
		REQUIRE(plan->type == LogicalOperatorType::LOGICAL_FILTER);
		REQUIRE(scan.table_filters.Equals(*initial_filters));
		REQUIRE(scan.bind_data->Cast<RepeatedPushdownData>().callback_filter_counts.empty());
	}
}
