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

//! g(a INTEGER, b BIGINT), g(a BIGINT, b INTEGER), g(a VARCHAR)
static ScalarFunctionSet GetOverloadedSet() {
	ScalarFunctionSet set("g");
	for (auto &parameters : vector<vector<LogicalType>> {{LogicalType::INTEGER, LogicalType::BIGINT},
	                                                     {LogicalType::BIGINT, LogicalType::INTEGER},
	                                                     {LogicalType::VARCHAR}}) {
		FunctionSignature sig;
		sig.AddParameter("a", parameters[0]);
		if (parameters.size() > 1) {
			sig.AddParameter("b", parameters[1]);
		}
		sig.SetReturnType(LogicalType::INTEGER);
		set.AddFunction(ScalarFunction("g", std::move(sig), NoopExec));
	}
	return set;
}

TEST_CASE("Selecting a function maps every argument to the parameter that receives it", "[api][function_binding]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto set = GetTemplatedSet();

	vector<LogicalType> positional {LogicalType::LIST(LogicalType::INTEGER), LogicalType::INTEGER, LogicalType::VARCHAR,
	                                LogicalType::VARCHAR};
	vector<pair<Identifier, LogicalType>> named {{"opt", LogicalType::BIGINT}, {"extra", LogicalType::BOOLEAN}};
	auto selection = FunctionBinder::SelectFunction(*con.context, set, positional, named);
	REQUIRE(selection.GetResult() == FunctionSelectionResult::SUCCESS);
	REQUIRE(selection.GetBinding().GetOverloadIndex() == 0);
	REQUIRE(!selection.GetError().HasError());

	// list, value, the two arguments "*args" receives, the keyword-only parameter, and "**kwargs"
	auto &binding = selection.GetBinding();
	const vector<idx_t> expected_parameters {0, 1, 2, 2, 3, 4};
	REQUIRE(binding.argument_parameters == expected_parameters);
	const vector<LogicalType> expected_types {LogicalType::LIST(LogicalType::INTEGER), LogicalType::INTEGER,
	                                          LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::ANY};
	REQUIRE(binding.parameter_types == expected_types);
	REQUIRE(binding.return_type == LogicalType::INTEGER);
}

TEST_CASE("Selecting a function deduces the templates of the overload", "[api][function_binding]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto set = GetTemplatedSet();

	// the template unifies the element type of the list with the value
	auto selection = FunctionBinder::SelectFunction(*con.context, set,
	                                                {LogicalType::LIST(LogicalType::INTEGER), LogicalType::BIGINT});
	REQUIRE(selection.IsSuccess());
	auto &parameter_types = selection.GetBinding().parameter_types;
	REQUIRE(parameter_types[0] == LogicalType::LIST(LogicalType::BIGINT));
	REQUIRE(parameter_types[1] == LogicalType::BIGINT);
	REQUIRE(selection.GetBinding().return_type == LogicalType::BIGINT);

	// arguments that determine conflicting types for the template do not match the overload
	selection =
	    FunctionBinder::SelectFunction(*con.context, set, {LogicalType::LIST(LogicalType::DATE), LogicalType::BOOLEAN});
	REQUIRE(selection.GetResult() == FunctionSelectionResult::NO_MATCH);
	REQUIRE(selection.GetFailedCandidates()[0].reason == FunctionMismatchReason::TEMPLATE_DEDUCTION);
	REQUIRE(StringUtil::Contains(selection.GetError().RawMessage(), "Cannot deduce template type 'T'"));

	// without a client context, the template is unified with the built-in implicit casts
	selection =
	    FunctionBinder::SelectFunction(nullptr, set, {LogicalType::LIST(LogicalType::INTEGER), LogicalType::BIGINT});
	REQUIRE(selection.IsSuccess());
	REQUIRE(selection.GetBinding().return_type == LogicalType::BIGINT);

	// a template that neither the arguments nor the defaults determine does not match either
	FunctionSignature sig;
	sig.AddParameter("value", LogicalType::INTEGER);
	sig.SetReturnType(LogicalType::TEMPLATE("T"));
	ScalarFunctionSet undetermined("u");
	undetermined.AddFunction(ScalarFunction("u", std::move(sig), NoopExec));
	selection = FunctionBinder::SelectFunction(*con.context, undetermined, {LogicalType::INTEGER});
	REQUIRE(selection.GetResult() == FunctionSelectionResult::NO_MATCH);
	REQUIRE(selection.GetFailedCandidates()[0].reason == FunctionMismatchReason::TEMPLATE_UNRESOLVED);
	REQUIRE(StringUtil::Contains(selection.GetError().RawMessage(), "is not determined by the arguments"));
}

TEST_CASE("A call no overload matches records why each overload fails", "[api][function_binding]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto set = GetOverloadedSet();

	auto selection = FunctionBinder::SelectFunction(*con.context, set, {LogicalType::DATE});
	REQUIRE(selection.GetResult() == FunctionSelectionResult::NO_MATCH);
	REQUIRE(!selection.IsSuccess());
	REQUIRE(selection.GetCandidates().empty());
	REQUIRE(selection.GetError().HasError());
	REQUIRE(StringUtil::Contains(selection.GetError().RawMessage(), "No function matches"));

	auto &failed = selection.GetFailedCandidates();
	REQUIRE(failed.size() == 3);
	// the last cannot take a DATE as its VARCHAR, which is closer than the first two leaving "b" unfilled
	REQUIRE(failed[0].GetOverloadIndex() == 2);
	REQUIRE(failed[0].reason == FunctionMismatchReason::INCOMPATIBLE_ARGUMENT);
	REQUIRE(failed[0].position.GetIndex() == 0);
	REQUIRE(failed[1].GetOverloadIndex() == 0);
	REQUIRE(failed[1].reason == FunctionMismatchReason::MISSING_PARAMETER);
	REQUIRE(failed[1].parameter.GetIndex() == 1);
	REQUIRE(!failed[1].position.IsValid());
	REQUIRE(failed[2].GetOverloadIndex() == 1);
	REQUIRE(failed[0].distance < failed[1].distance);
	REQUIRE(failed[1].distance == failed[2].distance);
	REQUIRE(StringUtil::Contains(selection.GetError().RawMessage(),
	                             "Closest candidate: g(a VARCHAR) -> INTEGER (argument 1 of type DATE cannot be "
	                             "implicitly cast to VARCHAR)"));

	// too many arguments, and a name no overload has
	selection = FunctionBinder::SelectFunction(*con.context, set,
	                                           {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR});
	REQUIRE(selection.GetFailedCandidates()[2].GetOverloadIndex() == 2);
	REQUIRE(selection.GetFailedCandidates()[2].reason == FunctionMismatchReason::TOO_MANY_ARGUMENTS);
	// the two overloads taking two arguments are equally close, so neither is named
	REQUIRE(!StringUtil::Contains(selection.GetError().RawMessage(), "Closest candidate"));

	// h(a VARCHAR, b INTEGER := 0) takes two arguments, but none named "c"
	FunctionSignature sig;
	sig.AddParameter("a", LogicalType::VARCHAR);
	sig.AddParameter("b", LogicalType::INTEGER, Value::INTEGER(0));
	sig.SetReturnType(LogicalType::INTEGER);
	ScalarFunctionSet defaulted("h");
	defaulted.AddFunction(ScalarFunction("h", std::move(sig), NoopExec));
	selection =
	    FunctionBinder::SelectFunction(*con.context, defaulted, {LogicalType::VARCHAR}, {{"c", LogicalType::INTEGER}});
	REQUIRE(selection.GetFailedCandidates()[0].reason == FunctionMismatchReason::UNKNOWN_NAMED_ARGUMENT);
	REQUIRE(selection.GetFailedCandidates()[0].position.GetIndex() == 1);
	// the only overload is not listed a second time
	auto &message = selection.GetError().RawMessage();
	REQUIRE(StringUtil::Contains(message, "Candidate function: h(a VARCHAR, b INTEGER := 0) -> INTEGER (it has no "
	                                      "parameter named \"c\")"));
	REQUIRE(!StringUtil::Contains(message, "Candidate functions"));
	REQUIRE(!StringUtil::Contains(message, "Closest candidate"));
}

TEST_CASE("Fewer arguments that cannot be cast are a closer miss", "[api][function_binding]") {
	DuckDB db(nullptr);
	Connection con(db);
	// k(a DATE, b DATE), k(a INTEGER, b DATE)
	ScalarFunctionSet set("k");
	for (auto &first : {LogicalType::DATE, LogicalType::INTEGER}) {
		FunctionSignature sig;
		sig.AddParameter("a", first);
		sig.AddParameter("b", LogicalType::DATE);
		sig.SetReturnType(LogicalType::INTEGER);
		set.AddFunction(ScalarFunction("k", std::move(sig), NoopExec));
	}

	auto selection = FunctionBinder::SelectFunction(*con.context, set, {LogicalType::INTEGER, LogicalType::BOOLEAN});
	REQUIRE(selection.GetResult() == FunctionSelectionResult::NO_MATCH);
	auto &failed = selection.GetFailedCandidates();
	REQUIRE(failed[0].GetOverloadIndex() == 1);
	REQUIRE(failed[0].position.GetIndex() == 1);
	REQUIRE(failed[0].distance == 1);
	REQUIRE(failed[1].position.GetIndex() == 0);
	REQUIRE(failed[1].distance == 2);
}

TEST_CASE("A call several overloads match at the same cost is ambiguous", "[api][function_binding]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto set = GetOverloadedSet();

	auto selection = FunctionBinder::SelectFunction(*con.context, set, {LogicalType::INTEGER, LogicalType::INTEGER});
	REQUIRE(selection.GetResult() == FunctionSelectionResult::AMBIGUOUS);
	REQUIRE(!selection.IsSuccess());
	REQUIRE(StringUtil::Contains(selection.GetError().RawMessage(), "Could not choose a best candidate"));
	REQUIRE(selection.GetCandidates().size() == 2);
	REQUIRE(selection.GetCandidates()[0].cost == selection.GetCandidates()[1].cost);
	REQUIRE(selection.GetFailedCandidates().size() == 1);
}
