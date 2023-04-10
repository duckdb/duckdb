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
	// either through target_len insertions
	// or source_len deletions
	if (source_len == 0) {
		return target_len * cost_insertion;
	} else if (target_len == 0) {
		return source_len * cost_deletion;
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

	for (idx_t source_idx = 0; source_idx <= source_len; source_idx++) {
		distance[source_idx + 1][1] = source_idx * cost_deletion;
	}
	for (idx_t target_idx = 1; target_idx <= target_len; target_idx++) {
		distance[1][target_idx + 1] = target_idx * cost_insertion;
	}
	for (idx_t source_idx = 0; source_idx < source_len; source_idx++) {
		// keeps track of the largest string indices of target string matching current source character
		// same as DB in LW paper
		idx_t largest_target_index_matching;
		largest_target_index_matching = 0;
		for (idx_t target_idx = 0; target_idx < target_len; target_idx++) {
			idx_t ii, jj;
			// cost associated to diagnoal shift in distance matrix
			// d in LW paper
			uint8_t cost_diagonal_shift;
			ii = largest_source_index_matching[target_str[target_idx]];
			jj = largest_target_index_matching;
			// if characters match, diagonal move costs nothing and we update our largest target index
			// otherwise move is substitution and costs as such
			if (source_str[source_idx] == target_str[target_idx]) {
				cost_diagonal_shift = 0;
				largest_target_index_matching = target_idx + 1;
			} else {
				cost_diagonal_shift = cost_substitution;
			}
			distance[source_idx + 2][target_idx + 2] =
			    MinValue(distance[source_idx + 1][target_idx + 1] + cost_diagonal_shift,
			             MinValue(distance[source_idx + 2][target_idx + 1] + cost_insertion,
			                      MinValue(distance[source_idx + 1][target_idx + 2] + cost_deletion,
			                               distance[ii][jj] + (source_idx - ii) * cost_deletion + cost_transposition +
			                                   (target_idx - jj) * cost_insertion)));
		}
		largest_source_index_matching[source_str[source_idx]] = source_idx + 1;
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
