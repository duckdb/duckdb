//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/rule/move_constants.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/rule.hpp"

namespace duckdb {

// The MoveConstantsRule moves constants to the same side of an expression, e.g. if we have an expression x + 1 = 5000
// then this will turn it into x = 4999.
class MoveConstantsRule : public Rule {
public:
	explicit MoveConstantsRule(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, vector<reference<Expression>> &bindings, bool &changes_made,
	                             bool is_root) override;
};

} // namespace duckdb
