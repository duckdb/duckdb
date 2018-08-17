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

#include <algorithm>
#include <vector>

#include "common/exception.hpp"
#include "common/internal_types.hpp"
#include "optimizer/rule.hpp"
#include "parser/expression/cast_expression.hpp"
#include "parser/expression/constant_expression.hpp"

namespace duckdb {

class CrossProductRewrite : public Rule {
  public:
	CrossProductRewrite() {
		root = make_unique_base<AbstractRuleNode, LogicalNodeType>(
		    LogicalOperatorType::FILTER);
		root->children.push_back(
		    make_unique_base<AbstractRuleNode, LogicalNodeType>(
		        LogicalOperatorType::CROSS_PRODUCT));
		root->child_policy = ChildPolicy::UNORDERED;
	}

	std::unique_ptr<LogicalOperator>
	Apply(LogicalOperator &root, std::vector<AbstractOperator> &bindings) {
		auto &filter = (LogicalFilter &)root;
		auto &cross_product =
		    *reinterpret_cast<LogicalCrossProduct *>(root.children[0].get());

		return nullptr;
		throw Exception("Implement cross product rewrite!");
		// check if there's any
	};
};

} // namespace duckdb
