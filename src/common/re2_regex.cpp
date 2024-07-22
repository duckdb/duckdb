#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector.hpp"
#include <memory>

#include "duckdb/common/re2_regex.hpp"
#include "re2/re2.h"

namespace duckdb_re2 {

static size_t GetMultibyteCharLength(const char c) {
	if ((c & 0x80) == 0) {
		return 1; // 1-byte character (ASCII)
	} else if ((c & 0xE0) == 0xC0) {
		return 2; // 2-byte character
	} else if ((c & 0xF0) == 0xE0) {
		return 3; // 3-byte character
	} else if ((c & 0xF8) == 0xF0) {
		return 4; // 4-byte character
	} else {
		return 0; // invalid UTF-8leading byte
	}
}

Regex::Regex(const std::string &pattern, RegexOptions options) {
	RE2::Options o;
	o.set_case_sensitive(options == RegexOptions::CASE_INSENSITIVE);
	regex = duckdb::make_shared_ptr<duckdb_re2::RE2>(StringPiece(pattern), o);
}

bool RegexSearchInternal(const char *input_data, size_t input_size, Match &match, const RE2 &regex, RE2::Anchor anchor,
                         size_t start, size_t end) {
	duckdb::vector<StringPiece> target_groups;
	auto group_count = duckdb::UnsafeNumericCast<size_t>(regex.NumberOfCapturingGroups() + 1);
	target_groups.resize(group_count);
	match.groups.clear();
	if (!regex.Match(StringPiece(input_data, input_size), start, end, anchor, target_groups.data(),
	                 duckdb::UnsafeNumericCast<int>(group_count))) {
		return false;
	}
	for (auto &group : target_groups) {
		GroupMatch group_match;
		group_match.text = group.ToString();
		group_match.position = group.data() != nullptr ? duckdb::NumericCast<uint32_t>(group.data() - input_data) : 0;
		match.groups.emplace_back(group_match);
	}
	return true;
}

bool RegexSearch(const std::string &input, Match &match, const Regex &regex) {
	auto input_sz = input.size();
	return RegexSearchInternal(input.c_str(), input_sz, match, regex.GetRegex(), RE2::UNANCHORED, 0, input_sz);
}

bool RegexMatch(const std::string &input, Match &match, const Regex &regex) {
	auto input_sz = input.size();
	return RegexSearchInternal(input.c_str(), input_sz, match, regex.GetRegex(), RE2::ANCHOR_BOTH, 0, input_sz);
}

bool RegexMatch(const char *start, const char *end, Match &match, const Regex &regex) {
	auto sz = duckdb::UnsafeNumericCast<size_t>(end - start);
	return RegexSearchInternal(start, sz, match, regex.GetRegex(), RE2::ANCHOR_BOTH, 0, sz);
}

bool RegexMatch(const std::string &input, const Regex &regex) {
	Match nop_match;
	auto input_sz = input.size();
	return RegexSearchInternal(input.c_str(), input_sz, nop_match, regex.GetRegex(), RE2::ANCHOR_BOTH, 0, input_sz);
}

duckdb::vector<Match> RegexFindAll(const std::string &input, const Regex &regex) {
	return RegexFindAll(input.c_str(), input.size(), regex.GetRegex());
}

duckdb::vector<Match> RegexFindAll(const char *input_data, size_t input_size, const RE2 &regex) {
	duckdb::vector<Match> matches;
	size_t position = 0;
	Match match;
	while (RegexSearchInternal(input_data, input_size, match, regex, RE2::UNANCHORED, position, input_size)) {
		if (match.length(0)) {
			position = match.position(0) + match.length(0);
		} else { // match.length(0) == 0
			auto next_char_length = GetMultibyteCharLength(input_data[match.position(0)]);
			if (!next_char_length) {
				throw duckdb::InvalidInputException("Invalid UTF-8 leading byte at position " +
				                                    std::to_string(match.position(0) + 1));
			}
			if (match.position(0) + next_char_length < input_size) {
				position = match.position(0) + next_char_length;
			} else {
				matches.emplace_back(match);
				break;
			}
		}
		matches.emplace_back(match);
	}
	return matches;
}

} // namespace duckdb_re2
