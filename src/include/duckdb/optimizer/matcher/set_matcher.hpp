//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/matcher/set_matcher.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {

class SetMatcher {
public:
	//! The policy used by the SetMatcher
	enum class Policy {
		//! All entries have to be matched, and the matches have to be ordered
		ORDERED,
		//! All entries have to be matched, but the order of the matches does not matter
		UNORDERED,
		//! Only some entries have to be matched, the order of the matches does not matter
		SOME,
	};

	/* The double {{}} in the intializer for excluded_entries is intentional, workaround for bug in gcc-4.9 */
	template <class T, class MATCHER>
	static bool MatchRecursive(vector<unique_ptr<MATCHER>> &matchers, vector<T *> &entries, vector<T *> &bindings,
	                           unordered_set<idx_t> excluded_entries, idx_t m_idx = 0) {
		if (m_idx == matchers.size()) {
			// matched all matchers!
			return true;
		}
		// try to find a match for the current matcher (m_idx)
		idx_t previous_binding_count = bindings.size();
		for (idx_t e_idx = 0; e_idx < entries.size(); e_idx++) {
			// first check if this entry has already been matched
			if (excluded_entries.find(e_idx) != excluded_entries.end()) {
				// it has been matched: skip this entry
				continue;
			}
			// otherwise check if the current matcher matches this entry
			if (matchers[m_idx]->Match(entries[e_idx], bindings)) {
				// m_idx matches e_idx!
				// check if we can find a complete match for this path
				// first add e_idx to the new set of excluded entries
				unordered_set<idx_t> new_excluded_entries;
				new_excluded_entries = excluded_entries;
				new_excluded_entries.insert(e_idx);
				// then match the next matcher in the set
				if (MatchRecursive(matchers, entries, bindings, new_excluded_entries, m_idx + 1)) {
					// we found a match for this path! success
					return true;
				} else {
					// we did not find a match! remove any bindings we added in the call to Match()
					bindings.erase(bindings.begin() + previous_binding_count, bindings.end());
				}
			}
		}
		return false;
	}

	template <class T, class MATCHER>
	static bool Match(vector<unique_ptr<MATCHER>> &matchers, vector<T *> &entries, vector<T *> &bindings,
	                  Policy policy) {
		if (policy == Policy::ORDERED) {
			// ordered policy, count has to match
			if (matchers.size() != entries.size()) {
				return false;
			}
			// now entries have to match in order
			for (idx_t i = 0; i < matchers.size(); i++) {
				if (!matchers[i]->Match(entries[i], bindings)) {
					return false;
				}
			}
			return true;
		} else {
			if (policy == Policy::UNORDERED && matchers.size() != entries.size()) {
				// unordered policy, count does not match: no match
				return false;
			} else if (policy == Policy::SOME && matchers.size() > entries.size()) {
				// some policy, every matcher has to match a unique entry
				// this is not possible if there are more matchers than entries
				return false;
			}
			// now perform the actual matching
			// every matcher has to match a UNIQUE entry
			// we perform this matching in a recursive way
			unordered_set<idx_t> excluded_entries;
			if (!MatchRecursive(matchers, entries, bindings, excluded_entries)) {
				return false;
			}
			return true;
		}
	}

	template <class T, class MATCHER>
	static bool Match(vector<unique_ptr<MATCHER>> &matchers, vector<unique_ptr<T>> &entries, vector<T *> &bindings,
	                  Policy policy) {
		// convert vector of unique_ptr to vector of normal pointers
		vector<T *> ptr_entries;
		for (auto &entry : entries) {
			ptr_entries.push_back(entry.get());
		}
		// then just call the normal match function
		return Match(matchers, ptr_entries, bindings, policy);
	}
};

} // namespace duckdb
