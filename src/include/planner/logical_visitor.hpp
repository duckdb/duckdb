//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// planner/logical_visitor.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

class LogicalAggregate;
class LogicalDistinct;
class LogicalFilter;
class LogicalGet;
class LogicalLimit;
class LogicalOrder;
class LogicalProjection;
class LogicalInsert;
class LogicalCopy;

//! The LogicalOperatorVisitor is an abstract base class that implements the
//! Visitor pattern on LogicalOperator.
class LogicalOperatorVisitor {
  public:
	virtual ~LogicalOperatorVisitor(){};

	virtual void Visit(LogicalAggregate &aggregate);
	virtual void Visit(LogicalDistinct &aggregate);
	virtual void Visit(LogicalFilter &filter);
	virtual void Visit(LogicalGet &filter);
	virtual void Visit(LogicalLimit &filter);
	virtual void Visit(LogicalOrder &filter);
	virtual void Visit(LogicalProjection &filter);
	virtual void Visit(LogicalInsert &insert);
	virtual void Visit(LogicalCopy &copy);

};
} // namespace duckdb
