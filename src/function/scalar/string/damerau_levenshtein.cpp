#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/scalar/string_functions.hpp"

#include <algorithm>
#include <ctype.h>
#include <map>

namespace duckdb {

// Lowrance-Wagner algorithm https://dl.acm.org/doi/pdf/10.1145/321879.321880
// Can't calculate as trivial mod to levenshtein algorithm
// as we need to potentially know about earlier in the string
static idx_t DamerauLevenshteinDistance(const string_t &txt, const string_t &tgt) {
	// costs associated with each type of edit, to aid readability
	constexpr uint8_t cost_substitution = 1;
	constexpr uint8_t cost_insertion = 1;
	constexpr uint8_t cost_deletion = 1;
	constexpr uint8_t cost_transposition = 1;
	const auto txt_len = txt.GetSize();
	const auto tgt_len = tgt.GetSize();

	// If one string is empty, the distance equals the length of the other string
	if (txt_len == 0) {
		return tgt_len;
	} else if (tgt_len == 0) {
		return txt_len;
	}

	const auto txt_str = txt.GetDataUnsafe();
	const auto tgt_str = tgt.GetDataUnsafe();

	// larger than the largest possible value:
	const auto inf = txt_len * cost_deletion + tgt_len * cost_insertion + 1;
	std::vector<std::vector<idx_t>> h(txt_len + 2, std::vector<idx_t>(tgt_len + 2, inf));
	std::map<char, idx_t> da;

	for (idx_t i = 0; i <= txt_len; i++) {
		// H[i, 0] = i * Wd
		h[i + 1][1] = i * cost_deletion;
	}
	for (idx_t j = 1; j <= tgt_len; j++) {
		// H[0, j] = j * Wi
		h[1][j + 1] = j * cost_insertion;
	}
	for (idx_t i = 1; i <= txt_len; i++) {
		idx_t db;
		db = 0;
		for (idx_t j = 1; j <= tgt_len; j++) {
			idx_t ii, jj, d;
			// offset as strings are 0-indexed
			ii = da[tgt_str[j - 1]];
			jj = db;
			if (txt_str[i - 1] == tgt_str[j - 1]) {
				d = 0;
				db = j;
			} else {
				d = cost_substitution; // Wc
			}
			h[i + 1][j + 1] =
			    MinValue(h[i][j] + d, MinValue(h[i + 1][j] + cost_insertion,
			                                   MinValue(h[i][j + 1] + cost_deletion,
			                                            h[ii][jj] + (i - ii - 1) * cost_deletion + cost_transposition +
			                                                (j - jj - 1) * cost_insertion)));
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
	                                               LogicalType::BIGINT, DamerauLevenshteinFunction));
	set.AddFunction(damerau_levenshtein);
}

} // namespace duckdb
