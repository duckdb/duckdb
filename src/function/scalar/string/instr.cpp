#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"

#include <string.h>
#include <ctype.h>
#include <unordered_map>
#include <algorithm>    // std::max

using namespace std;

namespace duckdb {

static int64_t instr(string_t haystack, string_t needle);

static uint64_t instr_kmp(const string_t &str, const string_t &pattern);
static vector<uint32_t> BuildKPMTable(const string_t &pattern);

static uint64_t instr_bm(const string_t &str, const string_t &pattern);
static unordered_map<char, uint32_t> BuildBMTable(const string_t &pattern);

struct InstrOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		return instr(left, right);
	}
};

struct InstrKMPOperator {
    template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
        return instr_kmp(left, right);
    }
};

struct InstrBMOperator {
    template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
        return instr_bm(left, right);
    }
};

static int64_t instr(string_t haystack, string_t needle) {
	int64_t string_position = 0;

	// Getting information about the needle and the haystack
	auto input_haystack = haystack.GetData();
	auto input_needle = needle.GetData();
	auto size_haystack = haystack.GetSize();
	auto size_needle = needle.GetSize();

	// Needle needs something to proceed
	if (size_needle > 0) {
		// Haystack should be bigger or equal size to the needle
		while (size_haystack >= size_needle) {
			// Increment and check continuation bytes: bit 7 should be set and 6 unset
			string_position += (input_haystack[0] & 0xC0) != 0x80;
			// Compare Needle to the Haystack
			if ((memcmp(input_haystack, input_needle, size_needle) == 0)) {
				return string_position;
			}
			size_haystack--;
			input_haystack++;
		}

		// Did not find the needle
		string_position = 0;
	}
	return string_position;
}

uint64_t instr_kmp(const string_t &str, const string_t &pattern) {
    auto str_size = str.GetSize();
    auto patt_size = pattern.GetSize();
    if(patt_size > str_size)
        return 0;

    idx_t idx_patt = 0;
    idx_t idx_str = 0;
    auto kmp_table = BuildKPMTable(pattern);

    auto str_data = str.GetData();
    auto patt_data = pattern.GetData();

    while(idx_str < str_size) {
        if(str_data[idx_str] == patt_data[idx_patt]) {
            ++idx_str;
            ++idx_patt;
            if(idx_patt == patt_size)
                return (idx_str - idx_patt + 1);
        } else {
            if(idx_patt > 0) {
                idx_patt = kmp_table[idx_patt - 1];
            } else {
                ++idx_str;
            }
        }
    }
    return 0;
}

static vector<uint32_t> BuildKPMTable(const string_t &pattern) {
    auto patt_size = pattern.GetSize();
    auto patt_data = pattern.GetData();
    vector<uint32_t> table(patt_size);
    table[0] = 0;
    idx_t i = 1;
    idx_t j = 0;
    while(i < patt_size) {
        if(patt_data[j] == patt_data[i]) {
            ++j;
            table[i] = j;
            ++i;
        } else if(j > 0) {
            j = table[j-1];
        } else {
            table[i] = 0;
            ++i;
        }
    }
    return table;
}

static uint64_t instr_bm(const string_t &str, const string_t &pattern) {
    auto str_size = str.GetSize();
    auto patt_size = pattern.GetSize();
    if(patt_size > str_size)
        return 0;

    auto bm_table = BuildBMTable(pattern);

    auto str_data = str.GetData();
    auto patt_data = pattern.GetData();
    idx_t skip;
    char patt_char, str_char;
	for(idx_t idx_str = 0; idx_str <= (str_size - patt_size); idx_str += skip) {
		skip = 0;
		for(int idx_patt = patt_size - 1; idx_patt >= 0; --idx_patt) {
			patt_char = patt_data[idx_patt];
			str_char = str_data[idx_str + idx_patt];
			if(patt_char != str_char) {
				if(bm_table.find(str_char) != bm_table.end()) {
					skip = bm_table[str_char];
				} else {
					skip = patt_size;
				}
				break;
			}
		}
		if(skip == 0)
			return idx_str + 1;
	}
	return 0;
}

static unordered_map<char, uint32_t> BuildBMTable(const string_t &pattern) {
	unordered_map<char, uint32_t> table;
	auto patt_data = pattern.GetData();
	uint32_t size_patt = pattern.GetSize();
	char ch;
	for(uint32_t i = 0; i < size_patt; ++i) {
		ch = patt_data[i];
		table[ch] = std::max((uint32_t)1, size_patt - i - 1);
	}
	return table;
}

void InstrFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("instr",                              // name of the function
	                               {SQLType::VARCHAR, SQLType::VARCHAR}, // argument list
	                               SQLType::BIGINT,                      // return type
	                               ScalarFunction::BinaryFunction<string_t, string_t, int64_t, InstrOperator, true>));

    set.AddFunction(ScalarFunction("instr_kmp",                              // name of the function
                                   {SQLType::VARCHAR, SQLType::VARCHAR}, // argument list
                                   SQLType::BIGINT,                      // return type
                                   ScalarFunction::BinaryFunction<string_t, string_t, int64_t, InstrKMPOperator, true>));

    set.AddFunction(ScalarFunction("instr_bm",                              // name of the function
                                   {SQLType::VARCHAR, SQLType::VARCHAR}, // argument list
                                   SQLType::BIGINT,                      // return type
                                   ScalarFunction::BinaryFunction<string_t, string_t, int64_t, InstrBMOperator, true>));

}

} // namespace duckdb
