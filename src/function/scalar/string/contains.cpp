#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"

#include <string.h>
#include <ctype.h>
#include <unordered_map>
#include <algorithm>    // std::max
#include <cstring>

using namespace std;

namespace duckdb {

ContainsKMPBindData::ContainsKMPBindData(unique_ptr<vector<uint32_t>> table, unique_ptr<string_t> pattern)
    : kmp_table(move(table)), pattern(move(pattern)) {
}

ContainsKMPBindData::~ContainsKMPBindData() {
}

unique_ptr<FunctionData> ContainsKMPBindData::Copy() {
	return make_unique<ContainsKMPBindData>(move(kmp_table), move(pattern));
}
static vector<uint32_t> BuildKPMTable(const string_t &pattern);


// BIND Boyer Moore----------------------------------------------------------------------
ContainsBMBindData::ContainsBMBindData(std::unique_ptr<unordered_map<char, uint32_t>> table,
		std::unique_ptr<string_t> pattern): bm_table(move(table)), pattern(move(pattern)) {
}

ContainsBMBindData::~ContainsBMBindData() {
}

unique_ptr<FunctionData> ContainsBMBindData::Copy() {
	return make_unique<ContainsBMBindData>(move(bm_table), move(pattern));
}

static unordered_map<char, uint32_t> BuildBMTable(const string_t &pattern);
//---------------------------------------------------------------------------------------

static bool contains_instr(string_t haystack, string_t needle);

struct ContainsOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		return contains_instr(left, right);
	}
};

static bool contains_strstr(const string_t &str, const string_t &pattern);

struct ContainsSTRSTROperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		return contains_strstr(left, right);
	}
};

static bool contains_instr(string_t haystack, string_t needle) {
	// Getting information about the needle and the haystack
	auto input_haystack = haystack.GetData();
	auto input_needle = needle.GetData();
	auto size_haystack = haystack.GetSize();
	auto size_needle = needle.GetSize();

	// Needle needs something to proceed
	if (size_needle > 0) {
		// Haystack should be bigger or equal size to the needle
		while (size_haystack >= size_needle) {
			// Compare Needle to the Haystack
			if ((memcmp(input_haystack, input_needle, size_needle) == 0)) {
				return true;
			}
			size_haystack--;
			input_haystack++;
		}
	}
	return false;
}

static bool contains_strstr(const string_t &str, const string_t &pattern) {
    auto str_data = str.GetData();
    auto patt_data = pattern.GetData();
	return (strstr(str_data , patt_data) != nullptr);
}

static bool contains_kmp(const string_t &str, const string_t &pattern, vector<uint32_t> &kmp_table) {
    auto str_size = str.GetSize();
    auto patt_size = pattern.GetSize();
    if(patt_size > str_size) {
        return false;
    }
    if(patt_size == 0) {
    	return true;
    }

    idx_t idx_patt = 0;
    idx_t idx_str = 0;
    auto str_data = str.GetData();
    auto patt_data = pattern.GetData();

    while(idx_str < str_size) {
        if(str_data[idx_str] == patt_data[idx_patt]) {
            ++idx_str;
            ++idx_patt;
            if(idx_patt == patt_size) {
                return true;
            }
        } else {
            if(idx_patt > 0) {
                idx_patt = kmp_table[idx_patt - 1];
            } else {
                ++idx_str;
            }
        }
    }
    return false;
}

static void contains_kmp_function(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &strings = args.data[0];
	auto &patterns = args.data[1];

	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (ContainsKMPBindData &)*func_expr.bind_info;

	RE2::Options options; //TODO remove these two lines
	options.set_log_errors(false);

	if (info.kmp_table) {
		// FIXME: this should be a unary loop
		UnaryExecutor::Execute<string_t, bool, true>(strings, result, args.size(), [&](string_t input) {
			return contains_kmp(input, *info.pattern, *info.kmp_table);
		});
	} else {
		BinaryExecutor::Execute<string_t, string_t, bool, true>(
				strings, patterns, result, args.size(), [&](string_t input, string_t pattern) {
			auto kmp_table = BuildKPMTable(pattern);
			return contains_kmp(input, pattern, kmp_table);
		});

	}
}

static unique_ptr<FunctionData> contains_kmp_get_bind_function(BoundFunctionExpression &expr,
                                                               ClientContext &context) {
	// pattern is the second argument. If its constant, we can already prepare the pattern and store it for later.
	assert(expr.children.size() == 2);
	if (expr.children[1]->IsScalar()) {
		Value pattern_str = ExpressionExecutor::EvaluateScalar(*expr.children[1]);
		if (!pattern_str.is_null && pattern_str.type == TypeId::VARCHAR) {
			auto pattern = make_unique<string_t>(pattern_str.str_value);
			auto table = BuildKPMTable(*pattern);
			auto kmp_table = make_unique<vector<uint32_t>>(move(table));
			return make_unique<ContainsKMPBindData>(move(kmp_table), move(pattern));
		}
	}
	return make_unique<ContainsKMPBindData>(nullptr, nullptr);
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

static bool contains_bm(const string_t &str, const string_t &pattern, unordered_map<char, uint32_t> &bm_table) {
    auto str_size = str.GetSize();
    auto patt_size = pattern.GetSize();
    if(patt_size > str_size) {
        return 0;
    }
    if(patt_size == 0) {
    	return true;
    }

//    auto bm_table = BuildBMTable(pattern);

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
			return true;
	}
	return false;
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

static void contains_bm_function(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &strings = args.data[0];
	auto &patterns = args.data[1];

	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (ContainsBMBindData &)*func_expr.bind_info;

	if (info.bm_table) {
		// FIXME: this should be a unary loop
		UnaryExecutor::Execute<string_t, bool, true>(strings, result, args.size(), [&](string_t input) {
			return contains_bm(input, *info.pattern, *info.bm_table);
		});
	} else {
		BinaryExecutor::Execute<string_t, string_t, bool, true>(
				strings, patterns, result, args.size(), [&](string_t input, string_t pattern) {
			auto bm_table = BuildBMTable(pattern);
			return contains_bm(input, pattern, bm_table);
		});

	}
}

static unique_ptr<FunctionData> contains_bm_get_bind_function(BoundFunctionExpression &expr,
                                                               ClientContext &context) {
	// pattern is the second argument. If its constant, we can already prepare the pattern and store it for later.
	assert(expr.children.size() == 2);
	if (expr.children[1]->IsScalar()) {
		Value pattern_str = ExpressionExecutor::EvaluateScalar(*expr.children[1]);
		if (!pattern_str.is_null && pattern_str.type == TypeId::VARCHAR) {
			auto pattern = make_unique<string_t>(pattern_str.str_value);
			auto table = BuildBMTable(*pattern);
			auto bm_table = make_unique<unordered_map<char, uint32_t>>(move(table));
			return make_unique<ContainsBMBindData>(move(bm_table), move(pattern));
		}
	}
	return make_unique<ContainsBMBindData>(nullptr, nullptr);
}

void ContainsFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("contains_instr",                              // name of the function
	                               {SQLType::VARCHAR, SQLType::VARCHAR}, // argument list
								   SQLType::BOOLEAN,                      // return type
	                               ScalarFunction::BinaryFunction<string_t, string_t, bool, ContainsOperator, true>));

	set.AddFunction(ScalarFunction("contains_strstr",                              // name of the function
	                               {SQLType::VARCHAR, SQLType::VARCHAR}, // argument list
								   SQLType::BOOLEAN,                      // return type
	                               ScalarFunction::BinaryFunction<string_t, string_t, bool, ContainsSTRSTROperator, true>));

	set.AddFunction(ScalarFunction("contains_kmp",
									{SQLType::VARCHAR, SQLType::VARCHAR},
									SQLType::BOOLEAN,
									contains_kmp_function, false, contains_kmp_get_bind_function));

	set.AddFunction(ScalarFunction("contains_bm",
									{SQLType::VARCHAR, SQLType::VARCHAR},
									SQLType::BOOLEAN,
									contains_bm_function, false, contains_bm_get_bind_function));
}

} // namespace duckdb
