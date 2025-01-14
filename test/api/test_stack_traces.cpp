#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/function/table_function.hpp"

using namespace duckdb;

#if defined(__GLIBC__) || defined(__APPLE__)
bool contains_stack_traces = true;
#else
bool contains_stack_traces = false;
#endif

namespace {
void setup_has_to_be_in_stack_trace(DuckDB &db) {
	struct FunctionInput final : TableFunctionData {
		unique_ptr<FunctionData> Copy() const override {
			return make_uniq<FunctionInput>();
		}

		bool Equals(const FunctionData &other) const override {
			return true;
		}
	};

	auto SuccessTFBinding = [](ClientContext &, TableFunctionBindInput &input, vector<LogicalType> &out_types,
	                           vector<std::string> &out_names) -> unique_ptr<FunctionData> {
		out_names.emplace_back("Success");
		out_types.emplace_back(LogicalType::BOOLEAN);
		return make_uniq_base<FunctionData, FunctionInput>();
	};

	TableFunction table_fun(
	    "throw_exception", {},
	    [](ClientContext &, TableFunctionInput &, DataChunk &) {
		    throw Exception(ExceptionType::INTERNAL, "Test exception");
	    },
	    SuccessTFBinding);
	ExtensionUtil::RegisterFunction(*db.instance, table_fun);
}
} // namespace

TEST_CASE("Test stack traces are not overwritten", "[stack_traces]") {
	DuckDB db(nullptr);
	setup_has_to_be_in_stack_trace(db);
	if (!contains_stack_traces) {
		return;
	}

	Connection con(db);
	auto res = con.Query("CALL throw_exception()");
	REQUIRE_FAIL(res);

	auto error = res->GetErrorObject();
	REQUIRE_THAT(error.Message(), Catch::Contains("setup_has_to_be_in_stack_trace"));
}
