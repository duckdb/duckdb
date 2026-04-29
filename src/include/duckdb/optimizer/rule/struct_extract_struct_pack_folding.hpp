//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/rule/struct_extract_struct_pack_folding.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/rule.hpp"

namespace duckdb {

//! Fold struct_extract(struct_pack(c0, c1, ...), key) to the bound child expression for `key`.
//! This keeps table subscript forms (e.g. alias['col']) from materializing full-row structs,
//! restoring column pruning for scans (e.g. remote parquet).
class StructExtractStructPackFoldingRule : public Rule {
public:
	explicit StructExtractStructPackFoldingRule(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, vector<reference<Expression>> &bindings, bool &changes_made,
	                             bool is_root) override;
};

} // namespace duckdb
