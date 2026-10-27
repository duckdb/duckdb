#include "duckdb/parser/peg/matcher.hpp"
#include "duckdb/parser/peg/matcher/list_matcher.hpp"

namespace duckdb {

MatchStep MatchStep::Child(MatchInput input) {
	return MatchStep(input, nullopt);
}

MatchStep MatchStep::Complete(MatcherResult result) {
	return MatchStep(nullopt, result);
}

optional<MatchInput> MatchStep::GetChild() {
	return child;
}

MatcherResult MatchStep::GetResult() const {
	D_ASSERT(!child);
	if (!result) {
		throw InternalException("Completed match step has no result");
	}
	return *result;
}

class AtomicMatchProcess : public MatchProcess {
public:
	AtomicMatchProcess(const AtomicMatcher &matcher_p, MatchState &state_p) : matcher(matcher_p), state(state_p) {
	}

	MatchStep Resume(optional<MatcherResult> child_result) override {
		D_ASSERT(!child_result);
		D_ASSERT(!completed);
		completed = true;
		return MatchStep::Complete(matcher.MatchAtomic(state));
	}

private:
	const AtomicMatcher &matcher;
	MatchState &state;
	bool completed = false;
};

arena_ptr<MatchProcess> AtomicMatcher::StartMatch(MatchState &state) const {
	return state.Make<AtomicMatchProcess>(*this, state);
}

void ListMatcher::InitializeListChain() {
	first_child_is_chainable = false;
	if (!IsStructural() || suppress_suggestions || matchers.empty()) {
		return;
	}
	auto &first = matchers[0].get();
	if (!first.IsStructural() || first.Type() != MatcherType::LIST || first.IsPackratMemoized()) {
		return;
	}
	first_child_is_chainable = !first.Cast<ListMatcher>().suppress_suggestions;
}

} // namespace duckdb
