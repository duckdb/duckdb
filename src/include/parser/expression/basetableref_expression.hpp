
#pragma once

#include "parser/expression/tableref_expression.hpp"

namespace duckdb {
	class BaseTableRefExpression : public TableRefExpression {
	  public:
		BaseTableRefExpression() : 
			TableRefExpression(TableReferenceType::BASE_TABLE) { }

		virtual std::string ToString() const { return std::string(); }

		std::string database_name;
		std::string schema_name;
		std::string table_name;
	};
}
