//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// optimizer/logical_rules/index_scan_rewrite_rule.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/rule.hpp"

namespace duckdb {

class IndexScanRewriteRule : public Rule {
  public:
	IndexScanRewriteRule();

	std::unique_ptr<LogicalOperator>
	Apply(Rewriter &rewriter, LogicalOperator &op_root,
	      std::vector<AbstractOperator> &bindings, bool &fixed_point);
};

} // namespace duckdb
