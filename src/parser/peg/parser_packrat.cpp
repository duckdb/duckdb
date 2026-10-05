#include "duckdb/parser/peg/parser_packrat.hpp"

#include "duckdb/parser/peg/matcher.hpp"

namespace duckdb {

size_t ParserPackratKeyHash::operator()(const ParserPackratKey &key) const {
	return std::hash<idx_t>()(key.matcher_id) ^ (std::hash<idx_t>()(key.token_index) << 1);
}

ParserPackratCache::ParserPackratCache() = default;

ParserPackratCache::~ParserPackratCache() = default;

optional_ptr<const ParserPackratEntry> ParserPackratCache::Lookup(const Matcher &matcher, idx_t token_index) const {
	D_ASSERT(matcher.IsPackratMemoized());
	auto packrat_id = matcher.GetPackratId();
	D_ASSERT(packrat_id.IsValid());
	auto matcher_id = packrat_id.GetIndex();
	ParserPackratKey key {matcher_id, token_index};
	auto entry = entries.find(key);
	if (entry == entries.end()) {
		return nullptr;
	}
	return optional_ptr<const ParserPackratEntry>(&entry->second);
}

void ParserPackratCache::Store(const Matcher &matcher, idx_t token_index, ParserPackratEntry entry) {
	D_ASSERT(matcher.IsPackratMemoized());
	auto packrat_id = matcher.GetPackratId();
	D_ASSERT(packrat_id.IsValid());
	auto matcher_id = packrat_id.GetIndex();
	ParserPackratKey key {matcher_id, token_index};
	entries.insert(make_pair(key, entry));
}

} // namespace duckdb
