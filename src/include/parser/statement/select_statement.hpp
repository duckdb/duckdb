
#pragma once

#include <vector>

#include "parser/statement/sql_statement.hpp"

#include "parser/expression/abstract_expression.hpp"

namespace duckdb {
	class SelectStatement : public SQLStatement {
	  public:
		SelectStatement()
		    : SQLStatement(StatementType::SELECT), union_select(nullptr),
		      select_distinct(false){};
		virtual ~SelectStatement() {}

		virtual std::string ToString() const;

		std::vector<std::unique_ptr<AbstractExpression>> select_list;
		std::unique_ptr<AbstractExpression> from_table;
		bool select_distinct;

		std::unique_ptr<SelectStatement> union_select;
	};
}
