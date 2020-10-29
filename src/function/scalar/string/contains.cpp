#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"

#include <ctype.h>
#include <cstring>

using namespace std;

namespace duckdb {

static bool contains_strstr(string_t &str, string_t &pattern);

struct ContainsOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		return contains_strstr(left, right);
	}
};

static bool contains_strstr(string_t &str, string_t &pattern) {
	auto str_data = str.GetTerminatedData();
	auto patt_data = pattern.GetTerminatedData();
	return (strstr((const char *)str_data.get(), (const char *)patt_data.get()) != nullptr);
}

ScalarFunction ContainsFun::GetFunction() {
	return ScalarFunction("contains",                                   // name of the function
	                      {LogicalType::VARCHAR, LogicalType::VARCHAR}, // argument list
	                      LogicalType::BOOLEAN,                         // return type
	                      ScalarFunction::BinaryFunction<string_t, string_t, bool, ContainsOperator, true>);
}

void ContainsFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(GetFunction());
}

} // namespace duckdb
