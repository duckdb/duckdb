#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/string_util.hpp"

#include <ctype.h>
#include <algorithm>

namespace duckdb {

// See: https://www.kdnuggets.com/2020/10/optimizing-levenshtein-distance-measuring-text-similarity.html
// And: Iterative 2-row algorithm: https://en.wikipedia.org/wiki/Levenshtein_distance
// Note: A first implementation using the array algorithm version resulted in an error raised by duckdb (too muach memory usage) 
static int levenshtein2(const string& txt, const string& tgt) 
    {
    if (txt.size() < 1 || tgt.size() < 1) 
    {
        return -1; // too short
    }
	if (txt.size() > 254 || tgt.size() > 254) 
    {
        return -2; // too long
    }

    std::vector<int> distances(txt.size() + 1, 0);

    for (int pos = 1; pos <= (int)txt.size(); pos++) 
    {
        distances[pos] = std::min(distances[pos - 1], pos - 1);
        if (txt[pos - 1] != tgt[0]) 
        {
            distances[pos] += 1;
        } 
    }

    int dist = 0;
	int tempDist = 0;

    for (int pos2 = 1; pos2 < (int)tgt.size(); pos2++) 
    {
        dist = pos2 + 1;
        for (int pos1 = 1; pos1 <= (int)txt.size(); pos1++) 
        {
            tempDist = std::min(dist, std::min(distances[pos1 - 1], distances[pos1]) );
            if (txt[pos1 - 1] != tgt[pos2]) {
                tempDist += 1;
            } 
            distances[pos1 - 1] = dist;
            dist = tempDist;
        }
        distances[txt.size()] = dist;
    }

    return dist;
}


static int64_t LevenshteinScalarFunction(Vector &result, const string_t str, string_t tgt) {
	//const string atgt = utility::conversions::to_utf8string(tgt) ;
	const string atgt = tgt.GetString();
	//const string astr = utility::conversions::to_utf8string(str);
	const string astr = str.GetString();
	int64_t dist = (int64_t)levenshtein2(astr, atgt);
	return dist;
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
