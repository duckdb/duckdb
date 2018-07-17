
#pragma once

#include "parser/statement/select_statement.hpp"
#include "parser/expression/tableref_expression.hpp"

namespace duckdb {
	class SubqueryExpression : public TableRefExpression {
	  public:
		SubqueryExpression() : 
			TableRefExpression(TableReferenceType::SUBQUERY) { }

		virtual std::string ToString() const { return std::string(); }

		std::unique_ptr<SelectStatement> subquery;
	};
}
