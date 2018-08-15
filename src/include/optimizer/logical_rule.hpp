//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/parser.hpp
//
// Author: Hannes Mühleisen & Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>

#include "planner/logical_operator.hpp"

namespace duckdb {

class LogicalNode {
  public:
	LogicalOperatorType root;
	std::vector<std::unique_ptr<LogicalNode>> children;
	ChildPolicy child_policy;

	LogicalNode(LogicalOperatorType root)
	    : root(root), child_policy(ChildPolicy::UNORDERED) {}
	LogicalNode() : child_policy(ChildPolicy::ANY) {}
	virtual bool Matches(LogicalOperator &rel) = 0;
	virtual ~LogicalNode() {}
};

class LogicalNodeSet : public LogicalNode {
  public:
	std::vector<LogicalOperatorType> types;
	LogicalNodeSet(std::vector<LogicalOperatorType> types)
	    : types(types) {}
	virtual bool Matches(LogicalOperator &rel) {
		return std::find(types.begin(), types.end(), rel.type) != types.end();
	}
};

class LogicalNodeType : public LogicalNode {
  public:
	LogicalOperatorType type;
	LogicalNodeType(LogicalOperatorType type) : type(type) {}
	virtual bool Matches(LogicalOperator &rel) { return rel.type == type; }
};

class LogicalNodeAny : public LogicalNode {
  public:
	virtual bool Matches(LogicalOperator &rel) { return true; }
};

class LogicalRule {
  public:
	std::unique_ptr<LogicalNode> root;
	virtual std::unique_ptr<LogicalOperator>
	Apply(LogicalOperator &root,
	      std::vector<LogicalOperator *> &bindings) = 0;
	virtual ~LogicalRule() {}
};

} // namespace duckdb
