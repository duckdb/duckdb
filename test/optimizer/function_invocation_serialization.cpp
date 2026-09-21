#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/window_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/parser/parsed_data/create_window_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_secure_view.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/function/scalar/compressed_materialization_utils.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"

#include "duckdb/optimizer/remove_unused_columns.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_window.hpp"

using namespace duckdb;

namespace {

static void RequireSameValues(QueryResult &expected, QueryResult &actual) {
	REQUIRE_NO_FAIL(actual);
	REQUIRE(expected.GetTypes() == actual.GetTypes());
	REQUIRE(expected.RowCount() == actual.RowCount());
	for (idx_t row = 0; row < expected.RowCount(); row++) {
		for (idx_t col = 0; col < expected.ColumnCount(); col++) {
			REQUIRE(Value::NotDistinctFrom(expected.GetValue(col, row), actual.GetValue(col, row)));
		}
	}
}

} // namespace

TEST_CASE("Current list serialization preserves bound ordering across repeated copies",
          "[serialization][function_invocation]") {
	DuckDB db(nullptr);
	Connection connection(db);
	bool optimize = false;
	SECTION("Bound plan") {
	}
	SECTION("Optimized plan") {
		optimize = true;
	}
	REQUIRE_NO_FAIL(connection.Query("SET threads=1; SET debug_disable_optimizer=true; "
	                                 "CREATE TABLE ordering_input(l VARCHAR[]); "
	                                 "INSERT INTO ordering_input VALUES (['a','B',NULL,'a']),([]),(NULL)"));
	connection.BeginTransaction();
	for (const auto &expression : {"list_sort(l)", "list_sort(l, 'DESC')", "list_sort(l, 'DESC', 'NULLS LAST')",
	                               "list_reverse_sort(l)", "list_reverse_sort(l, 'NULLS LAST')", "list_grade_up(l)",
	                               "list_grade_up(l, 'DESC')", "list_grade_up(l, 'DESC', 'NULLS LAST')"}) {
		INFO(expression);
		REQUIRE_NO_FAIL(connection.Query("SET default_collation='nocase'; SET default_order='ASC'; "
		                                 "SET default_null_order='NULLS FIRST'"));
		auto sql = "SELECT " + string(expression) + " FROM ordering_input";
		auto expected = connection.Query(sql);
		REQUIRE_NO_FAIL(*expected);
		Parser parser(connection.context->GetParserOptions());
		parser.ParseQuery(sql);
		Planner planner(*connection.context);
		planner.CreatePlan(std::move(parser.statements[0]));
		if (optimize) {
			planner.plan->ResolveOperatorTypes();
			Optimizer optimizer(*planner.binder, *connection.context);
			planner.plan = optimizer.Optimize(std::move(planner.plan));
		}
		REQUIRE_NO_FAIL(connection.Query("SET default_collation=''; SET default_order='DESC'; "
		                                 "SET default_null_order='NULLS LAST'"));
		auto plan = std::move(planner.plan);
		for (idx_t copy = 0; copy < 2; copy++) {
			plan = plan->Copy(*connection.context);
		}
		plan->ResolveOperatorTypes();
		auto actual = connection.Query(make_uniq<LogicalPlanStatement>(std::move(plan)));
		RequireSameValues(*expected, *actual);
	}
	connection.Rollback();
}

static unique_ptr<LogicalOperator> PlanAndOptimize(Connection &connection, const string &sql) {
	Parser parser(connection.context->GetParserOptions());
	parser.ParseQuery(sql);
	Planner planner(*connection.context);
	planner.CreatePlan(std::move(parser.statements[0]));
	planner.plan->ResolveOperatorTypes();
	Optimizer optimizer(*planner.binder, *connection.context);
	auto plan = optimizer.Optimize(std::move(planner.plan));
	plan->ResolveOperatorTypes();
	return plan;
}

static void CheckCompressionOrigins(ClientContext &context, LogicalOperator &plan, vector<bool> &seen) {
	LogicalOperatorVisitor::EnumerateExpressions(plan, [&](unique_ptr<Expression> *root) {
		ExpressionIterator::VisitExpression<BoundFunctionExpression>(
		    **root, [&](const BoundFunctionExpression &expression) {
			    seen[idx_t(CMUtils::GetExpressionType(expression))] = true;
			    MemoryStream stream(Allocator::Get(context));
			    BinarySerializer::Serialize(expression, stream);
			    stream.Rewind();
			    bound_parameter_map_t parameters;
			    auto copy = BinaryDeserializer::Deserialize<Expression>(stream, context, parameters);
			    REQUIRE(CMUtils::GetExpressionType(copy->Cast<BoundFunctionExpression>()) ==
			            CMUtils::GetExpressionType(expression));
		    });
	});
	for (auto &child : plan.children) {
		CheckCompressionOrigins(context, *child, seen);
	}
}

TEST_CASE("Compressed materialization functions retain their identity across copies and serialization",
          "[serialization][function_invocation]") {
	DuckDB db(nullptr);
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("SET threads=1; CREATE TABLE compression_input AS "
	                                 "SELECT i::BIGINT i, i::VARCHAR s FROM range(100) r(i)"));
	connection.BeginTransaction();
	vector<bool> seen(4, false);
	for (const auto &sql :
	     {"SELECT i, count(*) FROM compression_input GROUP BY i",
	      "SELECT s, count(*) FROM compression_input GROUP BY s", "SELECT upper(s) FROM compression_input"}) {
		auto plan = PlanAndOptimize(connection, sql);
		CheckCompressionOrigins(*connection.context, *plan, seen);
	}
	for (auto found : seen) {
		REQUIRE(found);
	}
	connection.Rollback();
}

static optional_ptr<LogicalSecureView> FindSecureView(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_SECURE_VIEW) {
		return op.Cast<LogicalSecureView>();
	}
	for (auto &child : op.children) {
		auto view = FindSecureView(*child);
		if (view) {
			return view;
		}
	}
	return nullptr;
}

TEST_CASE("Secure-view caller predicates retain source positions across pruning and serialization",
          "[serialization][secure_view]") {
	DuckDB db(nullptr);
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("CREATE TABLE filter_input(i INTEGER, p VARCHAR); "
	                                 "INSERT INTO filter_input VALUES (1,'a'),(2,'b'),(NULL,'n'),(1,'a'); "
	                                 "CREATE SECURE VIEW filter_view AS SELECT * FROM filter_input"));
	connection.BeginTransaction();
	auto plan = PlanAndOptimize(connection, "SELECT p FROM filter_view WHERE i=1 ORDER BY p");
	for (idx_t copy = 0; copy < 2; copy++) {
		auto view = FindSecureView(*plan);
		REQUIRE(view);
		REQUIRE(view->source_filters.size() == 1);
		REQUIRE(view->source_filters.size() == view->pushed_filters.size());
		REQUIRE(view->source_filters[0]);
		idx_t references = 0;
		ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
		    *view->source_filters[0], [&](const BoundColumnRefExpression &ref) {
			    REQUIRE(ref.Depth() == 0);
			    REQUIRE(ref.Binding() == ColumnBinding(TableIndex(0), ProjectionIndex(0)));
			    references++;
		    });
		REQUIRE(references == 1);
		plan = plan->Copy(*connection.context);
		plan->ResolveOperatorTypes();
	}
	REQUIRE_NO_FAIL(connection.Query("SET debug_disable_optimizer=true"));
	auto result = connection.Query(make_uniq<LogicalPlanStatement>(std::move(plan)));
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {"a", "a"}));
	connection.Rollback();
}

TEST_CASE("Secure-view source positions survive window column pruning", "[serialization][secure_view]") {
	DuckDB db(nullptr);
	Connection connection(db);
	connection.BeginTransaction();
	for (idx_t retained_window = 0; retained_window < 2; retained_window++) {
		CAPTURE(retained_window);
		Parser parser(connection.context->GetParserOptions());
		parser.ParseQuery("SELECT row_number() OVER (), rank() OVER ()");
		Planner planner(*connection.context);
		planner.CreatePlan(std::move(parser.statements[0]));
		planner.plan->ResolveOperatorTypes();
		auto window = std::move(planner.plan->children[0]);
		REQUIRE(window->type == LogicalOperatorType::LOGICAL_WINDOW);
		REQUIRE(window->expressions.size() == 2);
		auto bindings = window->GetColumnBindings();
		auto types = window->types;
		auto source_position = bindings.size() - 2 + retained_window;
		vector<unique_ptr<Expression>> outputs;
		outputs.push_back(make_uniq<BoundColumnRefExpression>(types[source_position], bindings[source_position]));
		unique_ptr<LogicalOperator> plan =
		    make_uniq<LogicalProjection>(planner.binder->GenerateTableIndex(), std::move(outputs));
		plan->children.push_back(make_uniq<LogicalSecureView>(
		    "window_view", QualifiedName("memory", "main", "window_view"), types, nullptr, std::move(window)));
		Optimizer optimizer(*planner.binder, *connection.context);
		RemoveUnusedColumns remove(optimizer);
		remove.VisitOperator(plan);
		plan->ResolveOperatorTypes();
		for (idx_t copy = 0; copy < 2; copy++) {
			auto view = FindSecureView(*plan);
			REQUIRE(view);
			REQUIRE(view->children[0]->type == LogicalOperatorType::LOGICAL_WINDOW);
			REQUIRE(view->children[0]->expressions.size() == 1);
			auto selected = plan->expressions[0]->Cast<BoundColumnRefExpression>().Binding();
			idx_t matches = 0;
			for (idx_t i = 0; i < view->output_bindings.size(); i++) {
				if (view->output_bindings[i] != selected) {
					continue;
				}
				auto &source = view->output_expressions[i]->Cast<BoundColumnRefExpression>();
				REQUIRE(source.Binding() == ColumnBinding(TableIndex(0), ProjectionIndex(source_position)));
				matches++;
			}
			REQUIRE(matches == 1);
			plan = plan->Copy(*connection.context);
			plan->ResolveOperatorTypes();
		}
	}
	connection.Rollback();
}

TEST_CASE("Function unbind callbacks reconstruct retained invocation data", "[function_invocation]") {
	DuckDB db(nullptr);
	Connection connection(db);
	connection.BeginTransaction();
	for (const auto &sql : {"SELECT struct_insert({'a': 1}, \"new field\" := 2)", "SELECT alias(42)",
	                        "SELECT ([1,2,3])[:2]", "SELECT ([1,2,3])[2:]"}) {
		CAPTURE(sql);
		Parser parser(connection.context->GetParserOptions());
		parser.ParseQuery(sql);
		Planner planner(*connection.context);
		planner.CreatePlan(std::move(parser.statements[0]));
		auto &expression = *planner.plan->expressions[0];
		REQUIRE(expression.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION);
		auto &function = expression.Cast<BoundFunctionExpression>();
		auto &definition = *function.Function().GetDefinition();
		REQUIRE(definition.HasUnbindCallback());
		vector<unique_ptr<ParsedExpression>> children;
		for (auto &child : function.GetChildren()) {
			children.push_back(
			    ConstantExpression::FromValue(ExpressionExecutor::EvaluateScalar(*connection.context, *child)));
		}
		FunctionUnbindInput input(function, std::move(children));
		auto parsed = definition.GetUnbindCallback()(input);
		REQUIRE(parsed);
		auto expected = connection.Query(sql);
		auto actual = connection.Query("SELECT " + parsed->ToString());
		REQUIRE_NO_FAIL(*expected);
		RequireSameValues(*expected, *actual);
	}
	connection.Rollback();
}
TEST_CASE("Nested function qualification survives plan copies", "[serialization][function_invocation]") {
	DuckDB db(nullptr);
	Connection connection(db);
	connection.BeginTransaction();
	REQUIRE_NO_FAIL(connection.Query("CREATE SCHEMA parent; CREATE SCHEMA parent.child"));
	auto &context = *connection.context;
	auto &catalog = Catalog::GetCatalog(context, Identifier("memory"));
	auto install = [&](CreateFunctionInfo &info) {
		info.internal = false;
		info.SetQualifiedName(QualifiedName({catalog.GetName(), Identifier("parent"), Identifier("child")},
		                                    info.GetQualifiedName().Name()));
		catalog.CreateFunction(context, info);
	};
	auto &scalar = Catalog::GetEntry<ScalarFunctionCatalogEntry>(context, QualifiedName("system", "main", "abs"));
	CreateScalarFunctionInfo scalar_info(scalar.functions);
	install(scalar_info);
	auto &aggregate = Catalog::GetEntry<AggregateFunctionCatalogEntry>(context, QualifiedName("system", "main", "min"));
	CreateAggregateFunctionInfo aggregate_info(aggregate.functions);
	install(aggregate_info);
	auto &window =
	    Catalog::GetEntry<WindowFunctionCatalogEntry>(context, QualifiedName("system", "main", "row_number"));
	CreateWindowFunctionInfo window_info(window.functions);
	install(window_info);
	auto &table = Catalog::GetEntry<TableFunctionCatalogEntry>(context, QualifiedName("system", "main", "range"));
	CreateTableFunctionInfo table_info(table.functions);
	install(table_info);
	const string sql = "SELECT memory.parent.child.abs(i), memory.parent.child.min(i) OVER (), "
	                   "memory.parent.child.row_number() OVER () FROM memory.parent.child.range(3) r(i)";
	Parser parser(context.GetParserOptions());
	parser.ParseQuery(sql);
	Planner planner(context);
	planner.CreatePlan(std::move(parser.statements[0]));
	auto copy = planner.plan->Copy(context);
	copy = copy->Copy(context);
	copy->ResolveOperatorTypes();
	auto expected = connection.Query(sql);
	auto actual = connection.Query(make_uniq<LogicalPlanStatement>(std::move(copy)));
	REQUIRE_NO_FAIL(*expected);
	RequireSameValues(*expected, *actual);
	connection.Rollback();
}

TEST_CASE("Custom scan serialization retains invocation inputs", "[serialization][function_invocation]") {
	DuckDB db(nullptr);
	Connection connection(db);
	auto file = TestCreatePath("retained_scan_arguments.parquet");
	REQUIRE_NO_FAIL(connection.Query("COPY (SELECT 42 AS i) TO " + Value(file).ToSQLString() + " (FORMAT PARQUET)"));
	connection.BeginTransaction();
	auto plan =
	    PlanAndOptimize(connection, "SELECT * FROM read_parquet(" + Value(file).ToSQLString() + ", filename := true)");
	std::function<void(LogicalOperator &)> check = [&](LogicalOperator &op) {
		if (op.type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = op.Cast<LogicalGet>();
			REQUIRE(get.parameters.size() == 1);
			REQUIRE(get.parameters[0] == Value(file));
			REQUIRE(get.named_parameters.at("filename") == Value::BOOLEAN(true));
		}
		for (auto &child : op.children) {
			check(*child);
		}
	};
	check(*plan);
	for (idx_t i = 0; i < 2; i++) {
		plan = plan->Copy(*connection.context);
		check(*plan);
	}
	connection.Rollback();
}
