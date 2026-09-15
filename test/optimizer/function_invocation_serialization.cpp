#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_secure_view.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"

using namespace duckdb;

namespace {

class LegacyFunctionReader {
public:
	explicit LegacyFunctionReader(ScalarFunctionCatalogEntry &entry_p)
	    : entry(entry_p), functions(entry.functions.functions) {
		entry.functions.ApplyToFunctions([](ScalarFunction &function) {
			function.SetSerializeCallback(nullptr);
			function.SetDeserializeCallback(nullptr);
			function.SetLegacySerializeCallback(nullptr);
		});
	}
	~LegacyFunctionReader() {
		entry.functions.functions = std::move(functions);
	}

private:
	ScalarFunctionCatalogEntry &entry;
	vector<shared_ptr<const ScalarFunction>> functions;
};

static void RequireSameValues(MaterializedQueryResult &expected, MaterializedQueryResult &actual) {
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

TEST_CASE("Legacy function serialization supports readers without bind-data callbacks",
          "[serialization][function_invocation]") {
	DuckDB db(nullptr);
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("SET threads=1; SET debug_disable_optimizer=true; "
	                                 "CREATE TABLE legacy_input(i INTEGER, l INTEGER[]); "
	                                 "INSERT INTO legacy_input VALUES (1,[2,NULL,1,2]),(2,[]),(3,NULL)"));
	connection.BeginTransaction();
	for (const auto &name : {"alias", "list_sort", "list_reverse_sort", "list_grade_up"}) {
		auto &entry =
		    Catalog::GetEntry<ScalarFunctionCatalogEntry>(*connection.context, QualifiedName("system", "main", name));
		vector<string> arguments = {"l", "l, 'DESC'", "l, 'DESC', 'NULLS FIRST'"};
		if (string(name) == "alias") {
			arguments = {"i"};
		} else if (string(name) == "list_reverse_sort") {
			arguments = {"l", "l, 'NULLS FIRST'"};
		}
		for (auto &argument : arguments) {
			auto sql = "SELECT " + string(name) + "(" + argument + ") AS output FROM legacy_input ORDER BY i";
			INFO(sql);
			auto expected = connection.Query(sql);
			REQUIRE_NO_FAIL(*expected);
			Parser parser(connection.context->GetParserOptions());
			parser.ParseQuery(sql);
			Planner planner(*connection.context);
			planner.CreatePlan(std::move(parser.statements[0]));
			MemoryStream stream(Allocator::Get(*connection.context));
			SerializationOptions options;
			options.storage_compatibility = StorageCompatibility::FromIndex(StorageVersion::V1_5_0);
			BinarySerializer::Serialize(*planner.plan, stream, options);
			stream.Rewind();
			for (auto &function : entry.functions.functions) {
				REQUIRE(function->GetSerializeCallback());
				REQUIRE(function->GetDeserializeCallback());
			}
			// Emulate the legacy catalog definitions, which had no bind-data deserializer.
			LegacyFunctionReader legacy_reader(entry);
			bound_parameter_map_t parameters;
			auto copy = BinaryDeserializer::Deserialize<LogicalOperator>(stream, *connection.context, parameters);
			copy->ResolveOperatorTypes();
			auto actual = connection.Query(make_uniq<LogicalPlanStatement>(std::move(copy)));
			RequireSameValues(*expected, *actual);
		}
	}
	connection.Rollback();
}

TEST_CASE("Current list serialization preserves bound ordering across repeated copies",
          "[serialization][function_invocation]") {
	DuckDB db(nullptr);
	Connection connection(db);
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
			    seen[idx_t(expression.compression_origin)] = true;
			    MemoryStream stream(Allocator::Get(context));
			    BinarySerializer::Serialize(expression, stream);
			    stream.Rewind();
			    bound_parameter_map_t parameters;
			    auto copy = BinaryDeserializer::Deserialize<Expression>(stream, context, parameters);
			    REQUIRE(copy->Cast<BoundFunctionExpression>().compression_origin == expression.compression_origin);
		    });
	});
	for (auto &child : plan.children) {
		CheckCompressionOrigins(context, *child, seen);
	}
}

TEST_CASE("Compression origins survive serialization and omitted origins default to NONE",
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

TEST_CASE("List ordering retains its bound collation across plan serialization",
          "[serialization][function_invocation]") {
	DuckDB db(nullptr);
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("SET threads=1; SET default_collation='nocase'"));
	connection.BeginTransaction();
	auto plan = PlanAndOptimize(connection, "SELECT list_sort(x) FROM (VALUES (['b','B']::VARCHAR[]))t(x)");
	REQUIRE_NO_FAIL(connection.Query("SET default_collation=''"));
	plan = plan->Copy(*connection.context);
	plan->ResolveOperatorTypes();
	REQUIRE_NO_FAIL(connection.Query("SET debug_disable_optimizer=true"));
	auto direct = connection.Query(make_uniq<LogicalPlanStatement>(std::move(plan)));
	REQUIRE_NO_FAIL(*direct);
	REQUIRE(Value::NotDistinctFrom(direct->GetValue(0, 0), Value::LIST({Value("b"), Value("B")})));
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
