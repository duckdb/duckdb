//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/peg/matcher_stack.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/parser/peg/matcher.hpp"

namespace duckdb {

struct PackratMatchState {
	static bool IsEnabled(const Matcher &matcher, const MatchState &state) {
		return state.packrat_cache && matcher.IsPackratMemoized() && matcher.GetPackratId().IsValid();
	}

	optional<MatcherResult> TryLoadCachedResult(const Matcher &matcher, MatchState &state);
	void StoreResult(const Matcher &matcher, MatchState &state, const MatcherResult &result) const;

private:
	optional_idx token_index_before;
	idx_t max_token_index_before = 0;
};

struct MatchStackFrame {
public:
	explicit MatchStackFrame(MatchInput input);

public:
	bool IsInitialized() const;

public:
	const Matcher &matcher;
	MatchState &match_state;
	unique_ptr<MatchProcess> process;
	optional<MatcherResult> child_result;
	optional<MatcherResult> result;
	PackratMatchState packrat_state;
};

class MatchStack {
public:
	MatcherResult Execute(MatchInput input);

private:
	MatcherResult ExecuteAtomicMatcher(MatchInput input);
	void PushFrame(MatchInput input);
	void InitializeFrame(MatchStackFrame &frame);
	void ExecuteFrame(MatchStackFrame &frame);
	MatcherResult FinalizeFrame(MatchStackFrame &frame);

private:
	vector<unique_ptr<MatchStackFrame>> frames;
};

} // namespace duckdb
