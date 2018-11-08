//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// optimizer/logical_rules/cross_product_rewrite.hpp
//
// Author: Hannes Mühleisen & Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/rule.hpp"

namespace duckdb {

// TODO: this rule would be simplified by a matcher of filter ... any chain of
// children of type crossprod or join ... crosprod then we could apply multiple
// times instead of this mess

class CrossProductRewrite : public Rule {
  public:
	CrossProductRewrite();

	std::unique_ptr<LogicalOperator>
	Apply(Rewriter &rewriter, LogicalOperator &root,
	      std::vector<AbstractOperator> &bindings, bool &fixed_point);
};

} // namespace duckdb
