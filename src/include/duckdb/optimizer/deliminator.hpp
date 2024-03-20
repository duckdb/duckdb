//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/deliminator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/column_binding_replacer.hpp"

namespace duckdb {

struct DelimCandidate;

//! The Deliminator optimizer traverses the logical operator tree and removes any redundant DelimGets/DelimJoins
class Deliminator {
public:
	Deliminator() {
	}
	//! Perform DelimJoin elimination
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

private:
	//! Finds DelimJoins and their corresponding DelimGets
	void FindCandidates(unique_ptr<LogicalOperator> &op, vector<DelimCandidate> &candidates);
	void FindJoinWithDelimGet(unique_ptr<LogicalOperator> &op, DelimCandidate &candidate, idx_t depth = 0);
	//! Whether the DelimJoin is selective
	bool HasSelection(const LogicalOperator &delim_join);
	//! Remove joins with a DelimGet
	bool RemoveJoinWithDelimGet(LogicalComparisonJoin &delim_join, const idx_t delim_get_count,
	                            unique_ptr<LogicalOperator> &join, bool &all_equality_conditions);
	bool RemoveInequalityJoinWithDelimGet(LogicalComparisonJoin &delim_join, const idx_t delim_get_count,
	                                      unique_ptr<LogicalOperator> &join,
	                                      const vector<ReplacementBinding> &replacement_bindings);
	void TrySwitchSingleToLeft(LogicalComparisonJoin &delim_join);

private:
	optional_ptr<LogicalOperator> root;
};

} // namespace duckdb
