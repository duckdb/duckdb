#include "duckdb/parser/peg/parser_packrat.hpp"

#include "duckdb/parser/peg/matcher.hpp"

namespace duckdb {

size_t ParserPackratKeyHash::operator()(const ParserPackratKey &key) const {
	return std::hash<idx_t>()(key.matcher_id) ^ (std::hash<idx_t>()(key.token_index) << 1);
}

ParserPackratCache::ParserPackratCache() = default;

ParserPackratCache::~ParserPackratCache() = default;

MatcherResult ParserPackratCache::Match(const Matcher &matcher, MatchState &state) {
	D_ASSERT(matcher.IsPackratMemoized());
	if (!matcher.IsPackratMemoized()) {
		return matcher.MatchParseResultInternal(state);
	}
	auto packrat_id = matcher.GetPackratId();
	if (!packrat_id.IsValid()) {
		return matcher.MatchParseResultInternal(state);
	}

	auto matcher_id = packrat_id.GetIndex();
	ParserPackratKey key {matcher_id, state.token_iterator.Position()};
	auto entry = entries.find(key);
	if (entry != entries.end()) {
		auto &cached = entry->second;
		state.token_iterator.SetPosition(cached.token_index_after);
		state.max_token_index = MaxValue(state.max_token_index, cached.max_token_index_seen);
		if (cached.success) {
			return MatcherResult::Success(cached.result);
		}
		return MatcherResult::Failure();
	}

	auto max_token_index_before = state.GetMaxTokenIndex();
	auto result = matcher.MatchParseResultInternal(state);
	ParserPackratEntry cached;
	cached.success = result.IsSuccess();
	cached.token_index_after = state.token_iterator.Position();
	cached.max_token_index_seen = MaxValue(max_token_index_before, state.GetMaxTokenIndex());
	cached.result = result.GetParseResult();
	entries.insert(make_pair(key, cached));
	return result;
}

} // namespace duckdb
