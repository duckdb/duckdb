// RE2 compatibility layer with std::regex

#ifndef DUCKDB_RE2_REGEX_H
#define DUCKDB_RE2_REGEX_H

#include <vector>
#include <string>

#include "re2/re2.h"

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

bool RegexSearch(const std::string &input, Match &match, const Regex &regex);
bool RegexMatch(const std::string &input, Match &match, const Regex &regex);
bool RegexMatch(const char *start, const char *end, Match &match, const Regex &regex);
bool RegexMatch(const std::string &input, const Regex &regex);
std::vector<Match> RegexFindAll(const std::string &input, const Regex &regex);

}

#endif // DUCKDB_RE2_REGEX_H
