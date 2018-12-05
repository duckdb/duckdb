//===----------------------------------------------------------------------===//
//                         DuckDB
//
// optimizer/logical_rules/index_scan_rewrite_rule.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/rule.hpp"

namespace duckdb {

class IndexScanRewriteRule : public Rule {
public:
	IndexScanRewriteRule();

	unique_ptr<LogicalOperator> Apply(Rewriter &rewriter, LogicalOperator &op_root, vector<AbstractOperator> &bindings,
	                                  bool &fixed_point);
};

} // namespace duckdb
