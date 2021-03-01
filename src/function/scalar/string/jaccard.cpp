#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <iterator>
#include <ctype.h>
#include <map>

namespace duckdb {

inline std::map<string_t, int64_t> GetSet(string_t str) {
	std::map<string_t, int64_t> map_of_chars;
	idx_t str_len = str.GetSize();
	auto str_str = str.GetDataUnsafe();

	for (idx_t i = 0; i < str_len; i++) {
		map_of_chars.insert(make_pair(str_str[i], 1));
	}
	return map_of_chars;
}

inline std::map<string_t, int64_t> TabulateCharacters(std::map<string_t, int64_t> str,
                                                      std::map<string_t, int64_t> txt) {
	// use the shorter map to insert & update the count
	if (str.size() > txt.size()) {
		str.swap(txt);
	}
	std::map<string_t, int64_t>::iterator it = str.begin();
	string_t achar = "";
	while (it != str.end()) {
		achar = it->first;
		if (txt.count(achar) == 0) {
			txt[achar] = 1;
		} else {
			txt[achar]++;
		}
		it++;
	}

	return txt;
}

static float JaccardSimilarity(string_t str, string_t txt) {
	if (str.GetSize() < 1 || txt.GetSize() < 1) {
			throw InvalidInputException("Jaccard Function: An argument too short!");
		}
	std::map<string_t, int64_t> ms, mt, mu;
	ms = GetSet(str);
	mt = GetSet(txt);
	mu = TabulateCharacters(ms, mt); // for calculating union and intersection

	// get size of union
	int64_t size_union = mu.size();

	// get size of intersect -> counts > 1
	int64_t size_intersect = 0;
	std::map<string_t, int64_t>::iterator it = mu.begin();

	while (it != mu.end()) {
		if (it->second > 1) {
			size_intersect++;
		}
		it++;
	}

	return (float)size_intersect / (float)size_union;
}

static float JaccardScalarFunction(Vector &result, const string_t str, string_t tgt) {
	return (float)JaccardSimilarity(str, tgt);
}

static void JaccardFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &str_vec = args.data[0];
	auto &tgt_vec = args.data[1];

	BinaryExecutor::Execute<string_t, string_t, int64_t>(
	    str_vec, tgt_vec, result, args.size(),
	    [&](string_t str, string_t tgt) { return JaccardScalarFunction(result, str, tgt); });
}

void JaccardFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet jaccard("jaccard");
	jaccard.AddFunction(ScalarFunction("jaccard", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BIGINT,
	                                   JaccardFunction)); // Pointer to function implementation
	set.AddFunction(jaccard);
}

} // namespace duckdb
