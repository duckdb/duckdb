
#pragma once

#include "parser/expression/tableref_expression.hpp"

namespace duckdb {
class CrossProductExpression : public TableRefExpression {
  public:
	CrossProductExpression()
	    : TableRefExpression(TableReferenceType::CROSS_PRODUCT) {}

	virtual std::string ToString() const { return std::string(); }
};
}