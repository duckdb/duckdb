#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/map.hpp"

#include <ctype.h>

namespace duckdb {

static inline map<char, idx_t> GetSet(string_t str) {
	auto  map_of_chars = map<char, idx_t>{};
	idx_t str_len = str.GetSize();
	auto s = str.GetDataUnsafe();
	for (idx_t pos = 0; pos < str_len; pos++) {
		++map_of_chars[s[pos]];
	}
	return map_of_chars;
}

static inline map<char, idx_t> TabulateCharacters(map<char, idx_t> str, map<char, idx_t> txt) {
	auto tstr = map<char, idx_t>{};
	auto ttxt = map<char, idx_t>{};
	if (str.size() > txt.size()) {
		ttxt = str;
		tstr = txt;
	} else {
		ttxt = txt;
		tstr = str;
	}

	for (auto const &achar: tstr) {
		++ttxt[achar.first];
	}

// 	string_t achar;
// 	// for (map<string_t, idx_t>::iterator it = sstr.begin(); it != sstr.end(); ++it) {
// 	for (const auto &apair: tstr) {
// 		// achar = apair.first;
// 		// if (ttxt.count(achar) == 0) {
// 		// 	// ttxt.insert(make_pair(achar, 1));
// 		// } 
// 		// else {
// 		// 	ttxt.at(achar) += 1;
// 		// }
// 		// ++ttxt[apair.first];
// 		// achar = apair.first;
// 		// if (stxt.count(achar) == 1) {
// 			// ++stxt[achar];
			
// 		// } else {
// 		// 	/// stxt.insert(make_pair(achar, 1));
// 		// }
// 	}

	return ttxt;
}

static double JaccardSimilarity(const string_t &str, const string_t &txt) {
	if (str.GetSize() < 1 || txt.GetSize() < 1) {
			throw InvalidInputException("Jaccard Function: An argument too short!");
		}
	map<char, idx_t> ms, mt, mu;
	ms = GetSet(str);
	mt = GetSet(txt);
	mu = TabulateCharacters(ms, mt); // for calculating union and intersection

	// get size of union
	idx_t size_union = mu.size();

	// get size of intersect -> counts > 1
	idx_t size_intersect = 0;
	for (const auto &apair: mu) {
		if (apair.second > 1) {
			size_intersect++;
		}
	}

	return (double)size_intersect / (double)size_union;
	
	// return (float) mu.size();
	// return (float)1;
}

static double JaccardScalarFunction(Vector &result, const string_t str, string_t tgt) {
	return (float)JaccardSimilarity(str, tgt);
}

static void JaccardFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &str_vec = args.data[0];
	auto &tgt_vec = args.data[1];

	BinaryExecutor::Execute<string_t, string_t, double>(
	    str_vec, tgt_vec, result, args.size(),
	    [&](string_t str, string_t tgt) { return JaccardScalarFunction(result, str, tgt); });
}

void JaccardFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet jaccard("jaccard");
	jaccard.AddFunction(ScalarFunction("jaccard", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::DOUBLE,
	                                   JaccardFunction)); // Pointer to function implementation
	set.AddFunction(jaccard);
}

} // namespace duckdb
