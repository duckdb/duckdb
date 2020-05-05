#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/ternary_executor.hpp"

#include <string.h>
#include <ctype.h>
#include <unordered_map>
#include <algorithm>    // std::max
#include <iostream>

using namespace std;

namespace duckdb {

static string_t left_scalar_function(const string_t& str, const int64_t pos, vector<char> &result) {
	auto str_data = str.GetData();
	int64_t str_size = str.GetSize();

    if (pos >= 0) {
        return string_t(str_data, min(pos, str_size));
    }

    int64_t remaining = str_size + pos;
    return string_t(str_data, max(remaining, int64_t(0)));
}

static void (left_function)(DataChunk &args, ExpressionState &state, Vector &result) {
    assert(args.column_count() == 2 && args.data[0].type == TypeId::VARCHAR && args.data[1].type == TypeId::INT64);
    auto &str_vec = args.data[0];
    auto &pos_vec = args.data[1];

	vector<char> buffer;
	BinaryExecutor::Execute<string_t, int64_t, string_t>(str_vec, pos_vec, result, args.size(),
		[&](string_t str, int64_t pos) {
		    return StringVector::AddString(result, left_scalar_function(str, pos, buffer));
		}
	);
}

void LeftFun::RegisterFunction(BuiltinFunctions &set) {
    set.AddFunction(ScalarFunction("left", {SQLType::VARCHAR, SQLType::BIGINT}, SQLType::VARCHAR, left_function));
}

}
