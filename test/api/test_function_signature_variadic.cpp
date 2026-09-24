#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

using namespace duckdb;

static void CountArgumentsExec(DataChunk &args, ExpressionState &state, Vector &result) {
	result.Reference(Value::BIGINT(NumericCast<int64_t>(args.ColumnCount())), count_t(args.size()));
}

TEST_CASE("Parameters declared after *args are keyword-only", "[api][scalar_function]") {
	FunctionSignature sig;
	sig.AddParameter("a", LogicalType::INTEGER);
	sig.AddArgsParameter("args", LogicalType::INTEGER);
	sig.AddParameter("kw", LogicalType::INTEGER);
	sig.AddKwargsParameter("kwargs", LogicalType::ANY);
	sig.SetReturnType(LogicalType::BIGINT);
	REQUIRE_NOTHROW(sig.Verify());

	REQUIRE(sig.GetParameter(0).GetKind() == FunctionParameterKind::STANDARD);
	REQUIRE(sig.GetParameter(2).GetKind() == FunctionParameterKind::KEYWORD_ONLY);
	REQUIRE(sig.GetPositionalParameterCount() == 1);
	REQUIRE(sig.GetRequiredParameterCount() == 2);
	// the variadic parameters cannot be looked up by name
	REQUIRE(!sig.GetParameterIndexByName("args").IsValid());
	REQUIRE(sig.GetParameterIndexByName("kw").GetIndex() == 2);
	REQUIRE(sig.ToString() == "(a INTEGER, *args INTEGER, kw INTEGER, **kwargs ANY) -> BIGINT");
}

TEST_CASE("A keyword-only parameter closes the positional parameters", "[api][scalar_function]") {
	FunctionSignature sig;
	sig.AddParameter("a", LogicalType::INTEGER);
	sig.AddNamedParameter("kw", LogicalType::INTEGER, Value(LogicalType::INTEGER));
	sig.SetReturnType(LogicalType::BIGINT);
	REQUIRE_NOTHROW(sig.Verify());

	REQUIRE(sig.GetParameter(0).GetKind() == FunctionParameterKind::STANDARD);
	REQUIRE(sig.GetParameter(1).GetKind() == FunctionParameterKind::KEYWORD_ONLY);
	// no "*args" pack is declared, so the closure occupies no parameter slot of its own
	REQUIRE(sig.GetParameterCount() == 2);
	REQUIRE(!sig.HasVarArgs());
	REQUIRE(sig.GetArgsParameter() == nullptr);
	REQUIRE(sig.GetPositionalParameterCount() == 1);
	REQUIRE(sig.GetRequiredParameterCount() == 1);
	REQUIRE(sig.GetParameterIndexByName("kw").GetIndex() == 1);
	// it still reads as the bare "*" Python spells it with
	REQUIRE(sig.ToString() == "(a INTEGER, *, kw INTEGER := NULL) -> BIGINT");
}

TEST_CASE("A *args pack takes the place of the bare * separator", "[api][scalar_function]") {
	// the keyword-only parameter is closed off by the pack, so no "*" is spelled out on top of it
	FunctionSignature sig;
	sig.AddParameter("a", LogicalType::INTEGER);
	sig.AddArgsParameter("args", LogicalType::INTEGER);
	sig.AddNamedParameter("kw", LogicalType::INTEGER, Value(LogicalType::INTEGER));
	sig.SetReturnType(LogicalType::BIGINT);
	REQUIRE_NOTHROW(sig.Verify());
	REQUIRE(sig.HasVarArgs());
	REQUIRE(sig.ToString() == "(a INTEGER, *args INTEGER, kw INTEGER := NULL) -> BIGINT");
}

TEST_CASE("Keyword-only parameters still accept a **kwargs after them", "[api][scalar_function]") {
	FunctionSignature sig;
	sig.AddParameter("a", LogicalType::INTEGER);
	sig.AddNamedParameter("kw", LogicalType::INTEGER, Value(LogicalType::INTEGER));
	sig.AddKwargsParameter("kwargs", LogicalType::ANY);
	sig.SetReturnType(LogicalType::BIGINT);
	REQUIRE_NOTHROW(sig.Verify());
	REQUIRE(!sig.HasVarArgs());
	REQUIRE(sig.GetKwargsParameter()->GetType() == LogicalType::ANY);
	REQUIRE(sig.ToString() == "(a INTEGER, *, kw INTEGER := NULL, **kwargs ANY) -> BIGINT");
}

TEST_CASE("A function with only keyword-only parameters takes no positional argument", "[api][scalar_function]") {
	FunctionSignature sig;
	sig.AddNamedParameter("kw", LogicalType::INTEGER, Value(LogicalType::INTEGER));
	sig.SetReturnType(LogicalType::BIGINT);
	REQUIRE_NOTHROW(sig.Verify());
	REQUIRE(sig.GetPositionalParameterCount() == 0);
	REQUIRE(sig.ToString() == "(*, kw INTEGER := NULL) -> BIGINT");
}

TEST_CASE("A positional-only parameter cannot be passed by name", "[api][scalar_function]") {
	FunctionSignature sig;
	sig.AddPositionalOnlyParameter("a", LogicalType::INTEGER);
	sig.AddParameter("b", LogicalType::INTEGER);
	sig.AddKwargsParameter("kwargs", LogicalType::ANY);
	sig.SetReturnType(LogicalType::BIGINT);
	REQUIRE_NOTHROW(sig.Verify());

	REQUIRE(sig.GetParameter(0).GetKind() == FunctionParameterKind::POSITIONAL);
	REQUIRE(sig.GetParameter(1).GetKind() == FunctionParameterKind::STANDARD);
	// it takes a position like a standard parameter, but its name is invisible to a caller
	REQUIRE(sig.GetPositionalParameterCount() == 2);
	REQUIRE(sig.GetPositionalOnlyParameterCount() == 1);
	REQUIRE(!sig.GetParameterIndexByName("a").IsValid());
	REQUIRE(sig.GetParameterIndexByName("b").GetIndex() == 1);
	REQUIRE(sig.ToString() == "(a INTEGER, /, b INTEGER, **kwargs ANY) -> BIGINT");
}

TEST_CASE("Positional-only parameters combine with the other kinds", "[api][scalar_function]") {
	FunctionSignature sig;
	sig.AddPositionalOnlyParameter("a", LogicalType::INTEGER);
	sig.AddParameter("b", LogicalType::INTEGER);
	sig.AddArgsParameter("args", LogicalType::INTEGER);
	sig.AddParameter("kw", LogicalType::INTEGER, Value(LogicalType::INTEGER));
	sig.SetReturnType(LogicalType::BIGINT);
	REQUIRE_NOTHROW(sig.Verify());
	REQUIRE(sig.ToString() == "(a INTEGER, /, b INTEGER, *args INTEGER, kw INTEGER := NULL) -> BIGINT");

	// a signature of nothing but positional-only parameters still closes them
	FunctionSignature only;
	only.AddPositionalOnlyParameter("a", LogicalType::INTEGER);
	only.AddPositionalOnlyParameter("b", LogicalType::INTEGER);
	only.SetReturnType(LogicalType::BIGINT);
	REQUIRE_NOTHROW(only.Verify());
	REQUIRE(only.GetPositionalOnlyParameterCount() == 2);
	REQUIRE(only.ToString() == "(a INTEGER, b INTEGER, /) -> BIGINT");

	// the builder keeps them ahead of the parameters that can be passed by name
	FunctionSignature ordered;
	ordered.AddParameter("b", LogicalType::INTEGER);
	ordered.AddPositionalOnlyParameter("a", LogicalType::INTEGER);
	REQUIRE(ordered.GetParameter(0).GetName() == "a");
	REQUIRE_NOTHROW(ordered.Verify());
}

TEST_CASE("Positional-only parameters must come first", "[api][scalar_function]") {
	using Kind = FunctionParameterKind;
	auto i32 = LogicalType::INTEGER;
	auto verify = [&](vector<FunctionParameter> params) {
		FunctionSignature(std::move(params), LogicalType::BIGINT).Verify();
	};

	REQUIRE_NOTHROW(verify({{"a", i32, Kind::POSITIONAL}, {"b", i32}}));
	REQUIRE_THROWS(verify({{"b", i32}, {"a", i32, Kind::POSITIONAL}}));
	REQUIRE_THROWS(verify({{"args", i32, Kind::VAR_POSITIONAL}, {"a", i32, Kind::POSITIONAL}}));
	REQUIRE_THROWS(verify({{"kw", i32, Kind::KEYWORD_ONLY}, {"a", i32, Kind::POSITIONAL}}));
}

TEST_CASE("SetVarArgs declares *args and **kwargs of the same type", "[api][scalar_function]") {
	FunctionSignature sig;
	sig.AddParameter("a", LogicalType::INTEGER);
	sig.AddParameter(LogicalType::INTEGER);
	sig.SetVarArgs(LogicalType::VARCHAR);
	sig.SetReturnType(LogicalType::BIGINT);
	REQUIRE_NOTHROW(sig.Verify());

	REQUIRE(sig.HasVarArgs());
	REQUIRE(sig.GetVarArgs() == LogicalType::VARCHAR);
	REQUIRE(sig.GetPositionalParameterCount() == 2);
	REQUIRE(sig.GetParameter(1).GetName() == Identifier("col1"));
	REQUIRE(sig.GetKwargsParameter()->GetType() == LogicalType::VARCHAR);
	REQUIRE(sig.ToString() == "(a INTEGER, col1 INTEGER, *args VARCHAR, **kwargs VARCHAR) -> BIGINT");

	// the type can be replaced, and the varargs can be removed again
	sig.SetVarArgs(LogicalType::BIGINT);
	REQUIRE(sig.GetVarArgs() == LogicalType::BIGINT);
	REQUIRE(sig.GetParameterCount() == 4);
	sig.SetVarArgs(LogicalType(LogicalTypeId::INVALID));
	REQUIRE(!sig.HasVarArgs());
	REQUIRE(sig.GetParameterCount() == 2);
}

TEST_CASE("The order of the parameter kinds is verified", "[api][scalar_function]") {
	using Kind = FunctionParameterKind;
	auto i32 = LogicalType::INTEGER;
	auto verify = [&](vector<FunctionParameter> params) {
		FunctionSignature(std::move(params), LogicalType::BIGINT).Verify();
	};

	REQUIRE_NOTHROW(verify({{"a", i32}, {"args", i32, Kind::VAR_POSITIONAL}, {"kw", i32, Kind::KEYWORD_ONLY}}));
	// only one of each variadic parameter
	REQUIRE_THROWS(verify({{"x", i32, Kind::VAR_POSITIONAL}, {"y", i32, Kind::VAR_POSITIONAL}}));
	REQUIRE_THROWS(verify({{"x", i32, Kind::VAR_KEYWORD}, {"y", i32, Kind::VAR_KEYWORD}}));
	// **kwargs is last
	REQUIRE_THROWS(verify({{"x", i32, Kind::VAR_KEYWORD}, {"kw", i32, Kind::KEYWORD_ONLY}}));
	REQUIRE_THROWS(verify({{"x", i32, Kind::VAR_KEYWORD}, {"y", i32, Kind::VAR_POSITIONAL}}));
	// a standard parameter cannot follow *args or a keyword-only parameter
	REQUIRE_THROWS(verify({{"x", i32, Kind::VAR_POSITIONAL}, {"a", i32}}));
	REQUIRE_THROWS(verify({{"kw", i32, Kind::KEYWORD_ONLY}, {"a", i32}}));
	REQUIRE_THROWS(verify({{"kw", i32, Kind::KEYWORD_ONLY}, {"x", i32, Kind::VAR_POSITIONAL}}));
	// variadic parameters have no default value
	REQUIRE_THROWS(verify({{"x", i32, Value::INTEGER(1), Kind::VAR_POSITIONAL}}));

	// keyword-only parameters can mix required and optional parameters, standard parameters cannot
	FunctionSignature sig;
	sig.AddParameter("a", i32, Value::INTEGER(1));
	sig.AddArgsParameter("args", i32);
	sig.AddParameter("kw", i32, Value::INTEGER(1));
	sig.AddParameter("kw2", i32);
	REQUIRE_NOTHROW(sig.Verify());

	// the kinds are verified when the function is registered
	DuckDB db(nullptr);
	Connection con(db);
	ScalarFunction fn("bad_kinds", {{"x", i32, Kind::VAR_POSITIONAL}, {"a", i32}}, LogicalType::BIGINT,
	                  CountArgumentsExec);
	REQUIRE_THROWS(CreateScalarFunctionInfo(fn));
}

TEST_CASE("Argument names cannot be serialized to older storage versions", "[api][scalar_function]") {
	DBConfig config;
	config.SetOptionByName("storage_compatibility_version", Value("v1.0.0"));
	DuckDB db(nullptr, &config);
	Connection con(db);

	FunctionSignature sig;
	sig.AddArgsParameter("args", LogicalType::INTEGER);
	sig.AddParameter("kw", LogicalType::INTEGER);
	sig.SetReturnType(LogicalType::BIGINT);
	CreateScalarFunctionInfo info(ScalarFunction("count_arguments", std::move(sig), CountArgumentsExec));
	con.context->RunFunctionInTransaction(
	    [&]() { Catalog::GetSystemCatalog(*con.context).CreateFunction(*con.context, info); });

	REQUIRE_NO_FAIL(con.Query("SET debug_verify_serializer=true"));
	auto result = con.Query("SELECT count_arguments(i::INTEGER, kw := 1) FROM range(1) t(i)");
	REQUIRE(result->HasError());
	REQUIRE(StringUtil::Contains(result->GetError(), "cannot be serialized to a storage version older than"));
}

namespace {

//! The arguments the last bind of "defaults_probe" received
struct DefaultsProbe {
	vector<Value> inputs;
	named_parameter_map_t named_parameters;
};
DefaultsProbe defaults_probe;

unique_ptr<FunctionData> DefaultsProbeBind(ClientContext &, TableFunctionBindInput &input, vector<LogicalType> &types,
                                           vector<Identifier> &names) {
	defaults_probe.inputs = input.inputs;
	defaults_probe.named_parameters = input.named_parameters;
	types.push_back(LogicalType::BOOLEAN);
	names.emplace_back("ok");
	return make_uniq<TableFunctionData>();
}

void DefaultsProbeScan(ClientContext &, TableFunctionInput &, DataChunk &output) {
	output.SetCardinality(0);
}

} // namespace

TEST_CASE("A table function receives the declared default of every parameter", "[api][table_function]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.BeginTransaction();
	auto &context = *con.context;

	FunctionSignature sig;
	sig.AddParameter("a", LogicalType::INTEGER, Value::INTEGER(42));
	sig.AddNamedParameter("k", LogicalType::INTEGER, Value::INTEGER(7));
	sig.AddOptionalNamedParameter("o", LogicalType::INTEGER);
	// a type like ANY describes no value, so the option defaults to an untyped NULL
	sig.AddOptionalNamedParameter("untyped", LogicalType::ANY);
	TableFunction function("defaults_probe", std::move(sig), DefaultsProbeScan, DefaultsProbeBind);
	CreateTableFunctionInfo info(function);
	Catalog::GetSystemCatalog(context).CreateFunction(context, info);

	// every default is supplied, NULL ones included
	REQUIRE_NO_FAIL(con.Query("SELECT * FROM defaults_probe()"));
	REQUIRE(defaults_probe.inputs == vector<Value> {Value::INTEGER(42)});
	REQUIRE(defaults_probe.named_parameters.size() == 3);
	REQUIRE(defaults_probe.named_parameters.at("k") == Value::INTEGER(7));
	REQUIRE(defaults_probe.named_parameters.at("o").IsNull());
	REQUIRE(defaults_probe.named_parameters.at("o").type() == LogicalType::INTEGER);
	REQUIRE(defaults_probe.named_parameters.at("untyped").IsNull());
	REQUIRE(defaults_probe.named_parameters.at("untyped").type() == LogicalType::SQLNULL);

	// passed arguments replace the defaults, and a NULL is passed as a value - also in place of a default that is not
	// NULL
	REQUIRE_NO_FAIL(con.Query("SELECT * FROM defaults_probe(1, k := NULL, o := 2, untyped := 'x')"));
	REQUIRE(defaults_probe.inputs == vector<Value> {Value::INTEGER(1)});
	REQUIRE(defaults_probe.named_parameters.size() == 3);
	REQUIRE(defaults_probe.named_parameters.at("k").IsNull());
	REQUIRE(defaults_probe.named_parameters.at("o") == Value::INTEGER(2));
	REQUIRE(defaults_probe.named_parameters.at("untyped") == Value("x"));

	REQUIRE_NO_FAIL(con.Query("SELECT * FROM defaults_probe(a := 3, o := 4)"));
	REQUIRE(defaults_probe.inputs == vector<Value> {Value::INTEGER(3)});
	REQUIRE(defaults_probe.named_parameters.at("k") == Value::INTEGER(7));
	REQUIRE(defaults_probe.named_parameters.at("o") == Value::INTEGER(4));
	con.Rollback();
}
