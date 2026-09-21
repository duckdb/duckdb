#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

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
