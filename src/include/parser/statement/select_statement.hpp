
#pragma once

#include <vector>

#include "parser/statement/sql_statement.hpp"

#include "parser/expression/abstract_expression.hpp"

namespace duckdb {
struct GroupByDescription {
	std::vector<std::unique_ptr<AbstractExpression>> groups;
	std::unique_ptr<AbstractExpression> having;
};
struct OrderByNode {
	OrderType type;
	std::unique_ptr<AbstractExpression> expression;

	OrderByNode() {}
	OrderByNode(OrderType type, std::unique_ptr<AbstractExpression> expression)
	    : type(type), expression(std::move(expression)) {}
};
struct OrderByDescription {
	std::vector<OrderByNode> orders;
};
struct LimitDescription {
	int64_t limit = -1;
	int64_t offset = -1;
};

class SelectStatement : public SQLStatement {
  public:
	SelectStatement()
	    : SQLStatement(StatementType::SELECT), union_select(nullptr),
	      select_distinct(false){};
	virtual ~SelectStatement() {}

	virtual std::string ToString() const;

	std::vector<std::unique_ptr<AbstractExpression>> select_list;
	std::unique_ptr<AbstractExpression> from_table;
	std::unique_ptr<AbstractExpression> where_clause;
	bool select_distinct;

	GroupByDescription groupby;
	OrderByDescription orderby;
	LimitDescription limit;

	bool HasLimit() { return limit.limit > 0; }
	bool HasGroup() { return groupby.groups.size() > 0; }
	bool HasHaving() { return groupby.having.get(); }
	bool HasOrder() { return orderby.orders.size() > 0; }

	std::unique_ptr<SelectStatement> union_select;
};
}
