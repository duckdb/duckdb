
#pragma once

namespace duckdb {

class LogicalAggregate;
class LogicalDistinct;
class LogicalFilter;
class LogicalGet;
class LogicalLimit;
class LogicalOrder;

class LogicalOperatorVisitor {
  public:
	virtual ~LogicalOperatorVisitor(){};

  	virtual void Visit(LogicalAggregate& aggregate);
  	virtual void Visit(LogicalDistinct& aggregate);
  	virtual void Visit(LogicalFilter& filter);
  	virtual void Visit(LogicalGet& filter);
  	virtual void Visit(LogicalLimit& filter);
  	virtual void Visit(LogicalOrder& filter);
};
}
