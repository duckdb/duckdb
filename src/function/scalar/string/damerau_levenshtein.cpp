#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/scalar/string_functions.hpp"

#include <algorithm>
#include <ctype.h>
#include <map>

namespace duckdb {

// Using Lowrance-Wagner (LW) algorithm: https://doi.org/10.1145%2F321879.321880
// Can't calculate as trivial modification to levenshtein algorithm
// as we need to potentially know about earlier in the string
static idx_t DamerauLevenshteinDistance(const string_t &source, const string_t &target) {
	// costs associated with each type of edit, to aid readability
	constexpr uint8_t cost_substitution = 1;
	constexpr uint8_t cost_insertion = 1;
	constexpr uint8_t cost_deletion = 1;
	constexpr uint8_t cost_transposition = 1;
	const auto source_len = source.GetSize();
	const auto target_len = target.GetSize();

	// If one string is empty, the distance equals the length of the other string
	if (source_len == 0) {
		return target_len;
	} else if (target_len == 0) {
		return source_len;
	}

	const auto source_str = source.GetDataUnsafe();
	const auto target_str = target.GetDataUnsafe();

	// larger than the largest possible value:
	const auto inf = source_len * cost_deletion + target_len * cost_insertion + 1;
	// minimum edit distance from prefix of source string to prefix of target string
	// same object as H in LW paper (with indices offset by 1)
	std::vector<std::vector<idx_t>> distance(source_len + 2, std::vector<idx_t>(target_len + 2, inf));
	// keeps track of the largest string indices of source string matching each character
	// same as DA in LW paper
	std::map<char, idx_t> largest_source_index_matching;

	for (idx_t i = 0; i <= source_len; i++) {
		distance[i + 1][1] = i * cost_deletion;
	}
	for (idx_t j = 1; j <= target_len; j++) {
		distance[1][j + 1] = j * cost_insertion;
	}
	for (idx_t i = 1; i <= source_len; i++) {
		// keeps track of the largest string indices of target string matching current source character
		// same as DB in LW paper
		idx_t largest_target_index_matching;
		largest_target_index_matching = 0;
		for (idx_t j = 1; j <= target_len; j++) {
			idx_t ii, jj;
			// cost associated to diagnoal shift in distance matrix
			// d in LW paper
			uint8_t cost_diagonal_shift;
			// offset as strings are 0-indexed
			ii = largest_source_index_matching[target_str[j - 1]];
			jj = largest_target_index_matching;
			// if characters match, diagonal move costs nothing and we update our largest target index
			// otherwise move is substitution and costs as such
			if (source_str[i - 1] == target_str[j - 1]) {
				cost_diagonal_shift = 0;
				largest_target_index_matching = j;
			} else {
				cost_diagonal_shift = cost_substitution;
			}
			distance[i + 1][j + 1] =
			    MinValue(distance[i][j] + cost_diagonal_shift,
			             MinValue(distance[i + 1][j] + cost_insertion,
			                      MinValue(distance[i][j + 1] + cost_deletion,
			                               distance[ii][jj] + (i - ii - 1) * cost_deletion + cost_transposition +
			                                   (j - jj - 1) * cost_insertion)));
		}
		largest_source_index_matching[source_str[i - 1]] = i;
	}
	return distance[source_len + 1][target_len + 1];
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
