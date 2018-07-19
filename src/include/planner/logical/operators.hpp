
#pragma once

#include <vector>

#include "catalog/catalog.hpp"

#include "common/internal_types.hpp"
#include "common/printable.hpp"

#include "parser/expression/abstract_expression.hpp"
#include "parser/statement/select_statement.hpp"

namespace duckdb {
class LogicalOperator : public Printable {
  public:
	LogicalOperator(LogicalOperatorType type) : type(type) {}

	LogicalOperatorType GetOperatorType() { return type; }

	virtual std::string ToString() const override {
		std::string result = LogicalOperatorToString(type);
		if (children.size() > 0) {
			result += " ( ";
			for (auto& child : children) {
				result += child->ToString();
			}
			result += " )";
		}
		return result;
	}

	LogicalOperatorType type;
  	std::vector<std::unique_ptr<LogicalOperator>> children;
};

class LogicalGet : public LogicalOperator {
 public:
 	LogicalGet() : LogicalOperator(LogicalOperatorType::GET) {}
 	LogicalGet(std::shared_ptr<TableCatalogEntry> table) : LogicalOperator(LogicalOperatorType::GET), table(table) {}

 	std::shared_ptr<TableCatalogEntry> table;
};

class LogicalFilter : public LogicalOperator {
 public:
	LogicalFilter(std::unique_ptr<AbstractExpression> expression);

	std::vector<std::unique_ptr<AbstractExpression>> expressions;
 private:
 	void SplitPredicates(std::unique_ptr<AbstractExpression> expression);
};

class LogicalLimit : public LogicalOperator {
 public:
 	LogicalLimit(int64_t limit, int64_t offset) : 
 		LogicalOperator(LogicalOperatorType::LIMIT), limit(limit), offset(offset) { }

	int64_t limit;
	int64_t offset;
};

class LogicalOrder : public LogicalOperator {
 public:
 	LogicalOrder() : 
 		LogicalOperator(LogicalOperatorType::ORDER_BY) { }

	OrderByDescription description;
};

class LogicalDistinct : public LogicalOperator {
  public:
  	LogicalDistinct() : LogicalOperator(LogicalOperatorType::DISTINCT) { }
};

class LogicalAggregateAndGroupBy : public LogicalOperator {
 public:
 	LogicalAggregateAndGroupBy() : LogicalOperator(LogicalOperatorType::AGGREGATE_AND_GROUP_BY) { }

	std::vector<std::unique_ptr<AbstractExpression>> groups;
};

}
