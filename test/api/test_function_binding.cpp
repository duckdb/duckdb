#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/client_context.hpp"

using namespace duckdb;

static void NoopExec(DataChunk &args, ExpressionState &state, Vector &result) {
}

//! f(list T[], value T, *args VARCHAR, opt BIGINT := 0, **kwargs ANY) -> T
static ScalarFunctionSet GetTemplatedSet() {
	FunctionSignature sig;
	sig.AddParameter("list", LogicalType::LIST(LogicalType::TEMPLATE("T")));
	sig.AddParameter("value", LogicalType::TEMPLATE("T"));
	sig.AddArgs("args", LogicalType::VARCHAR);
	sig.AddKeywordOnly("opt", LogicalType::BIGINT, Value::BIGINT(0));
	sig.AddKwargs("kwargs", LogicalType::ANY);
	sig.SetReturnType(LogicalType::TEMPLATE("T"));
	ScalarFunctionSet set("f");
	set.AddFunction(ScalarFunction("f", std::move(sig), NoopExec));
	return set;
}

TEST_CASE("Selecting a function maps every argument to the parameter that receives it", "[api][function_binding]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto set = GetTemplatedSet();

	ErrorData error;
	vector<LogicalType> positional {LogicalType::LIST(LogicalType::INTEGER), LogicalType::INTEGER, LogicalType::VARCHAR,
	                                LogicalType::VARCHAR};
	vector<pair<Identifier, LogicalType>> named {{"opt", LogicalType::BIGINT}, {"extra", LogicalType::BOOLEAN}};
	auto binding = FunctionBinder::SelectFunction(*con.context, "f", set, positional, named, error);
	REQUIRE(binding.IsValid());
	REQUIRE(binding.GetIndex() == 0);
	REQUIRE(!error.HasError());

	// list, value, the two arguments "*args" receives, the keyword-only parameter, and "**kwargs"
	const vector<idx_t> expected_parameters {0, 1, 2, 2, 3, 4};
	REQUIRE(binding.argument_parameters == expected_parameters);
	const vector<LogicalType> expected_types {LogicalType::LIST(LogicalType::INTEGER),
	                                          LogicalType::INTEGER,
	                                          LogicalType::VARCHAR,
	                                          LogicalType::VARCHAR,
	                                          LogicalType::BIGINT,
	                                          LogicalType::ANY};
	REQUIRE(binding.parameter_types == expected_types);
}

TEST_CASE("Selecting a function substitutes the templates the arguments determine", "[api][function_binding]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto set = GetTemplatedSet();

	// the template unifies the element type of the list with the value
	ErrorData error;
	auto binding = FunctionBinder::SelectFunction(
	    *con.context, "f", set, {LogicalType::LIST(LogicalType::INTEGER), LogicalType::BIGINT}, error);
	REQUIRE(binding.IsValid());
	const vector<LogicalType> expected_types {LogicalType::LIST(LogicalType::BIGINT), LogicalType::BIGINT};
	REQUIRE(binding.parameter_types == expected_types);

	// a template that conflicts is left in place - binding the call reports the conflict
	binding = FunctionBinder::SelectFunction(*con.context, "f", set,
	                                         {LogicalType::LIST(LogicalType::DATE), LogicalType::BOOLEAN}, error);
	REQUIRE(binding.IsValid());
	REQUIRE(binding.parameter_types[0].IsTemplated());
	REQUIRE(binding.parameter_types[1].IsTemplated());
}

TEST_CASE("A call no overload matches selects no function", "[api][function_binding]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto set = GetTemplatedSet();

	ErrorData error;
	auto binding = FunctionBinder::SelectFunction(*con.context, "f", set, {LogicalType::INTEGER}, error);
	REQUIRE(!binding.IsValid());
	REQUIRE(binding.argument_parameters.empty());
	REQUIRE(error.HasError());
}
