#include "duckdb/parser/peg/parser_packrat.hpp"

#include "duckdb/parser/peg/matcher.hpp"

namespace duckdb {

ParserPackratCache::ParserPackratCache(idx_t start_token_index_p, idx_t slot_count_p)
    : start_token_index(start_token_index_p), slot_count(slot_count_p) {
}

ParserPackratCache::~ParserPackratCache() = default;

idx_t ParserPackratCache::GetEntryIndex(const Matcher &matcher, idx_t token_index) const {
	D_ASSERT(matcher.IsPackratMemoized());
	D_ASSERT(matcher.GetPackratSlot() < slot_count);
	D_ASSERT(token_index >= start_token_index);
	return (token_index - start_token_index) * slot_count + matcher.GetPackratSlot();
}

optional_ptr<const ParserPackratEntry> ParserPackratCache::Lookup(const Matcher &matcher, idx_t token_index) const {
	auto entry_index = GetEntryIndex(matcher, token_index);
	if (entry_index >= entries.size() || !entries[entry_index].valid) {
		return nullptr;
	}
	return optional_ptr<const ParserPackratEntry>(&entries[entry_index]);
}

void ParserPackratCache::Store(const Matcher &matcher, idx_t token_index, ParserPackratEntry entry) {
	auto entry_index = GetEntryIndex(matcher, token_index);
	if (entry_index >= entries.size()) {
		// grow geometrically so that a parse that keeps advancing does not re-allocate at every token
		auto new_size = MaxValue<idx_t>(entry_index + 1, entries.size() * 2);
		entries.resize(new_size);
	}
	auto &stored = entries[entry_index];
	if (stored.valid) {
		// keep the first result that was memoized for this position
		return;
	}
	stored = entry;
	stored.valid = true;
}

} // namespace duckdb
