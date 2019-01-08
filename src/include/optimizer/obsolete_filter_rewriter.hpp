//===----------------------------------------------------------------------===//
//                         DuckDB
//
// optimizer/obsolete_filter_rewriter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/rule.hpp"

namespace duckdb {

//! The ObsoleteFilterRewriter removes obsolete filter conditions(i.e. [X > 5 AND X > 7] => [X > 7]), removes nop
//! filters from the plan (i.e. WHERE TRUE) and prunes branches of the query tree that will always result in no results
//! (i.e. WHERE FALSE)
class ObsoleteFilterRewriter {
public:
	//! Perform obsolete filter rewriting recursively for the specified node
	unique_ptr<LogicalOperator> Rewrite(unique_ptr<LogicalOperator> node);
};

} // namespace duckdb
