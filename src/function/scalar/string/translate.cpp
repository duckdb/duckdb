#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/ternary_executor.hpp"

#include <string.h>
#include <ctype.h>
#include <unordered_map>
#include <unordered_set>

namespace duckdb {

string_t TranslateASCII(const string_t &haystack, const string_t &needle, const string_t &thread,
                        vector<char> &result) {
    // Get information about the haystack, the needle and the "thread"
    auto input_haystack = haystack.GetDataUnsafe();
    auto size_haystack = haystack.GetSize();

    auto input_needle = needle.GetDataUnsafe();
    auto size_needle = needle.GetSize();

    auto input_thread = thread.GetDataUnsafe();
    auto size_thread = thread.GetSize();

    // Reuse the buffer
    result.clear();

    // Character to be replaced
    unordered_map<char, char> to_replace;
    for (idx_t i = 0; i < size_needle && i < size_thread; i++) {
        char c = input_needle[i];
        // Ignore character that is existed.
        if (to_replace.count(c) != 0) {
            continue;
        }
        to_replace[c] = input_thread[i];
    }

    // Character to be deleted
    unordered_set<char> to_delete;
    for (idx_t i = size_thread; i < size_needle; i++) {
        char c = input_needle[i];
        if (to_replace.count(c) == 0) {
            to_delete.insert(input_needle[i]);
        }
    }

    for (idx_t i = 0; i < size_haystack; i++) {
        char c = input_haystack[i];
        if (to_replace.count(c) != 0) {
            result.push_back(to_replace[c]);
        } else if (to_delete.count(c) != 0) {
            continue;
        } else {
            result.push_back(c);
        }
    }

    return string_t(result.data(), result.size());
}

static string_t TranslateScalarFunction(const string_t &haystack, const string_t &needle, const string_t &thread,
                                      vector<char> &result) {
    return TranslateASCII(haystack, needle, thread, result);
}

static void TranslateFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &haystack_vector = args.data[0];
    auto &needle_vector = args.data[1];
    auto &thread_vector = args.data[2];

    vector<char> buffer;
    TernaryExecutor::Execute<string_t, string_t, string_t, string_t>(
        haystack_vector, needle_vector, thread_vector, result, args.size(),
        [&](string_t input_string, string_t needle_string, string_t thread_string) {
          return StringVector::AddString(result,
                                         TranslateScalarFunction(input_string, needle_string, thread_string, buffer));
        });
}

void TranslateFun::RegisterFunction(BuiltinFunctions &set) {
    set.AddFunction(ScalarFunction("translate",             // name of the function
                                   {LogicalType::VARCHAR, // argument list
                                    LogicalType::VARCHAR, LogicalType::VARCHAR},
                                   LogicalType::VARCHAR, // return type
                                   TranslateFunction));    // pointer to function implementation
}

} // namespace duckdb
