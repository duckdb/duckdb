#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/common/types/string_type.hpp"

#include "duckdb/common/exception.hpp"

using namespace std;

namespace duckdb {

static bool prefix(const string_t &str, const string_t &pattern);

struct PrefixOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		return prefix(left, right);
	}
};

static bool prefix(const string_t &str, const string_t &pattern) {
    uint32_t pattern_len = pattern.GetSize();
    if(pattern_len == 0)
        return false;

    uint32_t str_len = pattern.GetSize();
    if(pattern_len > str_len)
        return false;

    bool equal;
    uint32_t num_char_equals = 0;
    const char *str_data = str.GetData();
    const char *patt_data = pattern.GetData();

    for(idx_t i = 0; i < pattern_len; ++i) {
        equal = (str_data[i] == patt_data[i]); // removed branch
        num_char_equals += equal;
    }

    return (num_char_equals == pattern_len);
}


void PrefixFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("prefix",                             // name of the function
	                               {SQLType::VARCHAR, SQLType::VARCHAR}, // argument list
	                               SQLType::BOOLEAN,                     // return type
	                               ScalarFunction::BinaryFunction<string_t, string_t, bool, PrefixOperator, true>));
}

} // namespace duckdb
