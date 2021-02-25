#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/string_util.hpp"

#include <ctype.h>
#include <algorithm>

namespace duckdb {

// See: https://www.kdnuggets.com/2020/10/optimizing-levenshtein-distance-measuring-text-similarity.html
// And: Iterative 2-row algorithm: https://en.wikipedia.org/wiki/Levenshtein_distance
// Note: A first implementation using the array algorithm version resulted in an error raised by duckdb (too muach memory usage) 
// static idx_t levenshtein2(const string_t& txt, const string_t& tgt) 
//     {
//     auto txt_len = txt.GetSize();
//     auto tgt_len = tgt.GetSize();

//     if (txt_len < 1 ) throw InvalidInputException("Levenshtein Function: 1st argument too short");
//     if (tgt_len < 1 ) throw InvalidInputException("Levenshtein Function: 2nd argument too short");

//     auto txt_str = txt.GetDataUnsafe();
//     auto tgt_str = tgt.GetDataUnsafe();

//     std::vector<idx_t> distances(txt_len + 1, 0);

//     for (idx_t pos = 1; pos <= txt_len; pos++) 
//     {
//         distances[pos] = std::min(distances[pos - 1], pos - 1);
//         if (txt_str[pos - 1] != tgt_str[0]) 
//         {
//             distances[pos] += 1;
//         } 
//     }

//     idx_t dist = 0;
// 	idx_t tempDist = 0;

//     for (idx_t pos2 = 1; pos2 < tgt_len; pos2++) 
//     {
//         dist = pos2 + 1;
//         for (idx_t pos1 = 1; pos1 <= txt_len; pos1++) 
//         {
//             tempDist = std::min(dist, std::min(distances[pos1 - 1], distances[pos1]) );
//             if (txt_str[pos1 - 1] != tgt_str[pos2]) {
//                 tempDist += 1;
//             } 
//             distances[pos1 - 1] = dist;
//             dist = tempDist;
//         }
//         distances[txt_len] = dist;
//     }

//     return dist;
// }


// See: https://www.kdnuggets.com/2020/10/optimizing-levenshtein-distance-measuring-text-similarity.html
// And: Iterative 2-row algorithm: https://en.wikipedia.org/wiki/Levenshtein_distance
// Note: A first implementation using the array algorithm version resulted in an error raised by duckdb (too muach memory usage) 
static idx_t levenshtein_distance(const string_t& txt, const string_t& tgt) 
    {
    auto txt_len = txt.GetSize();
    auto tgt_len = tgt.GetSize();

    if (txt_len < 1 ) throw InvalidInputException("Levenshtein Function: 1st argument too short");
    if (tgt_len < 1 ) throw InvalidInputException("Levenshtein Function: 2nd argument too short");

    auto txt_str = txt.GetDataUnsafe();
    auto tgt_str = tgt.GetDataUnsafe();

    // Create two working vectors
    std::vector<idx_t> distances0(tgt_len + 1, 0);
    std::vector<idx_t> distances1(tgt_len + 1, 0);

    idx_t cost_substitution = 0; 
    idx_t cost_insertion = 0; 
    idx_t cost_deletion = 0; 

    // initialize distances0 vector
    // edit distance for an empty txt string is just the number of characters to delete from tgt
    for (idx_t pos_tgt = 0; pos_tgt <= tgt_len; pos_tgt++) {
        distances0[pos_tgt] = pos_tgt;
    }

    for (idx_t pos_txt = 0; pos_txt < txt_len; pos_txt++) {
        // calculate distances1 (current raw distances) from the previous row

        distances1[0] = pos_txt + 1;

        for (idx_t pos_tgt = 0; pos_tgt < tgt_len; pos_tgt++ ) {
            cost_deletion     = distances0[pos_tgt + 1] + 1;
            cost_insertion    = distances1[pos_tgt] + 1;
            cost_substitution = distances0[pos_tgt];

            if (txt_str[pos_txt] != tgt_str[pos_tgt]) {
                cost_substitution += 1;
            }

            distances1[pos_tgt + 1] = std::min(cost_deletion, 
                                                std::min(cost_substitution, cost_insertion));

        }
        // copy v1 (current row) to v0 (previous row) for next iteration
        // since data in v1 is always invalidated, a swap withou    t copy could be more efficient
        distances0 = distances1;
    }

    return distances0[tgt_len];

}


static int64_t LevenshteinScalarFunction(Vector &result, const string_t str, string_t tgt) {
	return (int64_t)levenshtein_distance(str, tgt);
}

static void LevenshteinFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &str_vec = args.data[0];
	auto &tgt_vec = args.data[1];

	BinaryExecutor::Execute<string_t, string_t, int64_t>(
	    str_vec, tgt_vec, result, args.size(),
	    [&](string_t str, string_t tgt) { return LevenshteinScalarFunction(result, str, tgt); });
}

void LevenshteinFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(
	    ScalarFunction("levenshtein", {
			LogicalType::VARCHAR, 
			LogicalType::VARCHAR
		}, 
		LogicalType::BIGINT, LevenshteinFunction)); // Pointer to function implementation
}

} // namespace duckdb
