// RE2 compatibility layer with std::regex

#ifndef DUCKDB_RE2_REGEX_H
#define DUCKDB_RE2_REGEX_H

#include <re2/re2.h>
#include <vector>
#include <string>

namespace duckdb_re2 {

enum class RegexOptions : uint8_t {
	NONE,
	CASE_INSENSITIVE
};

class Regex {
public:
	Regex(const std::string &pattern, RegexOptions options = RegexOptions::NONE) {
		RE2::Options o;
		o.set_case_sensitive(options == RegexOptions::CASE_INSENSITIVE);
		regex = std::make_shared<duckdb_re2::RE2>(StringPiece(pattern), o);
	}
	Regex(const char *pattern, RegexOptions options = RegexOptions::NONE) : Regex(std::string(pattern)) {
	}

	const duckdb_re2::RE2 &GetRegex() const {
		return *regex;
	}

private:
	std::shared_ptr<duckdb_re2::RE2> regex;
};


struct GroupMatch {
	std::string text;
	uint32_t position;

	const std::string& str() const {
		return text;
	}
	operator std::string() const {
		return text;
	}
};

struct Match {
	std::vector<GroupMatch> groups;

	GroupMatch &GetGroup(uint64_t index) {
		if (index >= groups.size()) {
			throw std::runtime_error("RE2: Match index is out of range");
		}
		return groups[index];
	}

	std::string str(uint64_t index) {
		return GetGroup(index).text;
	}

	uint64_t position(uint64_t index) {
		return GetGroup(index).position;
	}

	uint64_t length(uint64_t index) {
		return GetGroup(index).text.size();
	}

	GroupMatch& operator [](uint64_t i) {
		return GetGroup(i);
	}
};

static bool RegexSearchInternal(const char *input, Match &match, const Regex &r, RE2::Anchor anchor, size_t start, size_t end) {
	auto &regex = r.GetRegex();
	std::vector<StringPiece> target_groups;
	auto group_count = regex.NumberOfCapturingGroups();
	target_groups.resize(group_count + 1);
	match.groups.clear();
	if (!regex.Match(StringPiece(input), start, end, anchor, target_groups.data(), group_count)) {
		return false;
	}
	for(auto &group : target_groups) {
		GroupMatch group_match;
		group_match.text = group.ToString();
		group_match.position = group.data() - input;
		match.groups.emplace_back(group_match);
	}
	return true;
}

static bool RegexSearch(const std::string &input, Match &match, const Regex &regex) {
	return RegexSearchInternal(input.c_str(), match, regex, RE2::UNANCHORED, 0, input.size());
}

static bool RegexMatch(const std::string &input, Match &match, const Regex &regex) {
	return RegexSearchInternal(input.c_str(), match, regex, RE2::ANCHOR_BOTH, 0, input.size());
}

static bool RegexMatch(const char *start, const char *end, Match &match, const Regex &regex) {
	return RegexSearchInternal(start, match, regex, RE2::ANCHOR_BOTH, 0, end - start);
}

static bool RegexMatch(const std::string &input, const Regex &regex) {
	Match nop_match;
	return RegexSearchInternal(input.c_str(), nop_match, regex, RE2::ANCHOR_BOTH, 0, input.size());
}

static std::vector<Match> RegexFindAll(const std::string &input, const Regex &regex) {
	std::vector<Match> matches;
	size_t position = 0;
	Match match;
	while(RegexSearchInternal(input.c_str(), match, regex, RE2::UNANCHORED, position, input.size())) {
		position += match.position(0) + match.length(0);
		matches.emplace_back(std::move(match));
	}
	return matches;
}

}

#endif // DUCKDB_RE2_REGEX_H
