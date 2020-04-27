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

static bool contains_strstr(const string_t &str, const string_t &pattern);

struct ContainsOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		return contains_strstr(left, right);
	}
};

static bool contains_strstr(const string_t &str, const string_t &pattern) {
	auto str_data = str.GetData();
	auto patt_data = pattern.GetData();
	return (strstr(str_data, patt_data) != nullptr);
}

ScalarFunction ContainsFun::GetFunction() {
	return ScalarFunction("contains",                           // name of the function
	                      {SQLType::VARCHAR, SQLType::VARCHAR}, // argument list
	                      SQLType::BOOLEAN,                     // return type
	                      ScalarFunction::BinaryFunction<string_t, string_t, bool, ContainsOperator, true>);
}

void ContainsFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(GetFunction());
}

} // namespace duckdb
