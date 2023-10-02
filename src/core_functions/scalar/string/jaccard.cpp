#include "duckdb/core_functions/scalar/string_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/map.hpp"

#include <ctype.h>

namespace duckdb {

static inline map<char, idx_t> GetSet(const string_t &str) {
	auto map_of_chars = map<char, idx_t> {};
	idx_t str_len = str.GetSize();
	auto s = str.GetData();

	for (idx_t pos = 0; pos < str_len; pos++) {
		map_of_chars.insert(std::make_pair(s[pos], 1));
	}
	return map_of_chars;
}

static double JaccardSimilarity(const string_t &str, const string_t &txt) {
	if (str.GetSize() < 1 || txt.GetSize() < 1) {
		throw InvalidInputException("Jaccard Function: An argument too short!");
	}
	map<char, idx_t> m_str, m_txt;

	m_str = GetSet(str);
	m_txt = GetSet(txt);

	if (m_str.size() > m_txt.size()) {
		m_str.swap(m_txt);
	}

	for (auto const &achar : m_str) {
		++m_txt[achar.first];
	}
	// m_txt.size is now size of union.

	idx_t size_intersect = 0;
	for (const auto &apair : m_txt) {
		if (apair.second > 1) {
			size_intersect++;
		}
	}

	return (double)size_intersect / (double)m_txt.size();
}

static double JaccardScalarFunction(Vector &result, const string_t str, string_t tgt) {
	return (double)JaccardSimilarity(str, tgt);
}

static void JaccardFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &str_vec = args.data[0];
	auto &tgt_vec = args.data[1];

	BinaryExecutor::Execute<string_t, string_t, double>(
	    str_vec, tgt_vec, result, args.size(),
	    [&](string_t str, string_t tgt) { return JaccardScalarFunction(result, str, tgt); });
}

ScalarFunction JaccardFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::DOUBLE, JaccardFunction);
}

} // namespace duckdb
