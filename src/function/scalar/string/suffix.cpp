#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/common/types/string_type.hpp"

#include "duckdb/common/exception.hpp"

using namespace std;

namespace duckdb {

static bool suffix(const string_t &str, const string_t &suffix);

struct SuffixOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		return suffix(left, right);
	}
};

static bool suffix(const string_t &str, const string_t &suffix) {
	auto suffix_size = suffix.GetSize();
	auto str_size = str.GetSize();
	if (suffix_size > str_size) {
		return false;
	}

	auto suffix_data = suffix.GetData();
	auto str_data = str.GetData();
	int32_t suf_idx = suffix_size - 1;
	idx_t str_idx = str_size - 1;
	for (; suf_idx >= 0; --suf_idx, --str_idx) {
		if (suffix_data[suf_idx] != str_data[str_idx]) {
			return false;
		}
	}
	return true;
}

ScalarFunction SuffixFun::GetFunction() {
	return ScalarFunction("suffix",                             // name of the function
	                      {SQLType::VARCHAR, SQLType::VARCHAR}, // argument list
	                      SQLType::BOOLEAN,                     // return type
	                      ScalarFunction::BinaryFunction<string_t, string_t, bool, SuffixOperator, true>);
}

void SuffixFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(GetFunction());
}

} // namespace duckdb
