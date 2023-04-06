#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/string_util.hpp"

#include <ctype.h>
#include <algorithm>
#include <map>

namespace duckdb {

// Lowrance-Wagner algorithm https://dl.acm.org/doi/pdf/10.1145/321879.321880
// Can't calculate as trivial mod to levenshtein algorithm
// as we need to potentially know about earlier in the string

// TODO: formatting
// TODO: variables
static idx_t DamerauLevenshteinDistance(const string_t &txt, const string_t &tgt) {
	auto txt_len = txt.GetSize();
	auto tgt_len = tgt.GetSize();

	// If one string is empty, the distance equals the length of the other string
	if (txt_len == 0) {
		return tgt_len;
	} else if (tgt_len == 0) {
		return txt_len;
	}

	auto txt_str = txt.GetDataUnsafe();
	auto tgt_str = tgt.GetDataUnsafe();

	// larger than the largest possible value:
	auto inf = txt_len + tgt_len + 1;
	std::vector<std::vector<idx_t>> h(txt_len + 2, std::vector<idx_t>(tgt_len + 2, inf));
	std::map<char, idx_t> da;
	// TODO: sort out this complete mess of variables
	idx_t db;
	idx_t ii, jj, d;

	for (idx_t i = 0; i <= txt_len; i++) {
		// H[i, 0] = i * Wd
		h[i + 1][1] = i;
	}
	for (idx_t j = 1; j <= txt_len; j++) {
		// H[0, j] = j * Wi
		h[1][j + 1] = j;
	}
	for (idx_t i = 1; i <= txt_len; i++) {
		db = 0;
		for (idx_t j = 1; j <= tgt_len; j++) { 
			// offset as strings are 0-indexed
			ii = da[tgt_str[j - 1]];
			jj = db;
			if (txt_str[i - 1] == tgt_str[j - 1]) {
				d = 0;
				db = j;
			} else {
				d = 1;  // Wc
			}
			h[i + 1][j + 1] = MinValue(
				h[i][j] + d,
				MinValue(
					h[i + 1][j] + 1,  // wi
					MinValue(
						h[i][j + 1] + 1,  // wd
						h[ii][jj] + (i - ii - 1) + 1 + (j - jj - 1)
					)
				)
			);
		}
		da[txt_str[i - 1]] = i;
	}
	return h[txt_len + 1][tgt_len + 1];
}

static int64_t DamerauLevenshteinScalarFunction(Vector &result, const string_t str, const string_t tgt) {
	return (int64_t)DamerauLevenshteinDistance(str, tgt);
}

static void DamerauLevenshteinFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &str_vec = args.data[0];
	auto &tgt_vec = args.data[1];

	BinaryExecutor::Execute<string_t, string_t, int64_t>(
	    str_vec, tgt_vec, result, args.size(),
	    [&](string_t str, string_t tgt) { return DamerauLevenshteinScalarFunction(result, str, tgt); });
}

void DamerauLevenshteinFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet damerau_levenshtein("damerau_levenshtein");
	damerau_levenshtein.AddFunction(ScalarFunction("damerau_levenshtein", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                       LogicalType::BIGINT,
	                                       DamerauLevenshteinFunction));
	set.AddFunction(damerau_levenshtein);
}

} // namespace duckdb
