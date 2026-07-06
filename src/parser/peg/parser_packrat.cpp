#include "duckdb/parser/peg/parser_packrat.hpp"

#include "duckdb/parser/peg/matcher.hpp"

#include <cstdio>
#include <cstdlib>

namespace duckdb {

size_t ParserPackratKeyHash::operator()(const ParserPackratKey &key) const {
	return std::hash<idx_t>()(key.matcher_id) ^ (std::hash<idx_t>()(key.token_index) << 1);
}

ParserPackratCache::ParserPackratCache() = default;

ParserPackratCache::~ParserPackratCache() = default;

bool ParserPackratCache::Enabled() {
	auto env = std::getenv("DUCKDB_PARSER_PACKRAT");
	return !env || env[0] == '\0' || env[0] != '0';
}

bool ParserPackratCache::PrintStats() {
	auto env = std::getenv("DUCKDB_PARSER_PACKRAT_STATS");
	return env && env[0] != '\0' && env[0] != '0';
}

void ParserPackratCache::Reset() {
	entries.clear();
	stats = ParserPackratStats();
}

optional_ptr<ParseResult> ParserPackratCache::Match(const Matcher &matcher, MatchState &state) {
	if (!matcher.IsPackratMemoized()) {
		return matcher.MatchParseResultInternal(state);
	}
	auto packrat_id = matcher.GetPackratId();
	if (!packrat_id.IsValid()) {
		return matcher.MatchParseResultInternal(state);
	}

	auto matcher_id = packrat_id.GetIndex();
	ParserPackratKey key {matcher_id, state.token_index};
	auto entry = entries.find(key);
	if (entry != entries.end()) {
		stats.hits++;
		auto &cached = entry->second;
		state.token_index = cached.token_index_after;
		state.max_token_index = MaxValue(state.max_token_index, cached.max_token_index_seen);
		if (cached.success) {
			stats.success_hits++;
			return cached.result;
		}
		stats.failure_hits++;
		return nullptr;
	}

	stats.misses++;
	auto max_token_index_before = state.GetMaxTokenIndex();
	auto result = matcher.MatchParseResultInternal(state);
	ParserPackratEntry cached;
	cached.success = result != nullptr;
	cached.token_index_after = state.token_index;
	cached.max_token_index_seen = MaxValue(max_token_index_before, state.GetMaxTokenIndex());
	cached.result = result;
	if (cached.success) {
		stats.stored_success++;
	} else {
		stats.stored_failure++;
	}
	entries.insert(make_pair(key, cached));
	return result;
}

void ParserPackratCache::Print() const {
	std::fprintf(stderr,
	             "Parser packrat cache: %llu entries, %llu hits, %llu misses, %llu success_hits, "
	             "%llu failure_hits, %llu stored_success, %llu stored_failure\n",
	             static_cast<unsigned long long>(entries.size()), static_cast<unsigned long long>(stats.hits),
	             static_cast<unsigned long long>(stats.misses), static_cast<unsigned long long>(stats.success_hits),
	             static_cast<unsigned long long>(stats.failure_hits),
	             static_cast<unsigned long long>(stats.stored_success),
	             static_cast<unsigned long long>(stats.stored_failure));
}

} // namespace duckdb
