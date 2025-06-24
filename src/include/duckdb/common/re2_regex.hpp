// RE2 compatibility layer with std::regex

#pragma once

#include "duckdb/common/winapi.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"
#include <stdexcept>

namespace duckdb_re2 {
class RE2;

enum class RegexOptions : uint8_t { NONE, CASE_INSENSITIVE };

class Regex {
public:
	DUCKDB_API explicit Regex(const std::string &pattern, RegexOptions options = RegexOptions::NONE);
	explicit Regex(const char *pattern, RegexOptions options = RegexOptions::NONE) : Regex(std::string(pattern)) {
	}
	const duckdb_re2::RE2 &GetRegex() const {
		return *regex;
	}

private:
	duckdb::shared_ptr<duckdb_re2::RE2> regex;
};

struct GroupMatch {
	std::string text;
	uint32_t position;

	const std::string &str() const { // NOLINT
		return text;
	}
	operator std::string() const { // NOLINT: allow implicit cast
		return text;
	}
};

struct Match {
	duckdb::vector<GroupMatch> groups;

	GroupMatch &GetGroup(uint64_t index) {
		if (index >= groups.size()) {
			throw std::runtime_error("RE2: Match index is out of range");
		}
		return groups[index];
	}

	std::string str(uint64_t index) { // NOLINT
		return GetGroup(index).text;
	}

	uint64_t position(uint64_t index) { // NOLINT
		return GetGroup(index).position;
	}

	uint64_t length(uint64_t index) { // NOLINT
		return GetGroup(index).text.size();
	}

	GroupMatch &operator[](uint64_t i) {
		return GetGroup(i);
	}
};

DUCKDB_API bool RegexSearch(const std::string &input, Match &match, const Regex &regex);
DUCKDB_API bool RegexMatch(const std::string &input, Match &match, const Regex &regex);
DUCKDB_API bool RegexMatch(const char *start, const char *end, Match &match, const Regex &regex);
DUCKDB_API bool RegexMatch(const std::string &input, const Regex &regex);
DUCKDB_API duckdb::vector<Match> RegexFindAll(const std::string &input, const Regex &regex);
DUCKDB_API duckdb::vector<Match> RegexFindAll(const char *input_data, size_t input_size, const RE2 &regex);
} // namespace duckdb_re2
