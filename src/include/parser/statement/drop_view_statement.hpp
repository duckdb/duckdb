//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/statement/drop_view_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_data.hpp"
#include "parser/sql_statement.hpp"

namespace duckdb {

class DropViewStatement : public SQLStatement {
public:
	DropViewStatement() : SQLStatement(StatementType::DROP_VIEW), info(make_unique<DropViewInformation>()){};

	string ToString() const override {
		return "DROP TABLE";
	}

	bool Equals(const SQLStatement *other_) const override {
		if (!SQLStatement::Equals(other_)) {
			return false;
		}
		throw NotImplementedException("Equality not implemented!");
	}

	unique_ptr<DropViewInformation> info;
};

} // namespace duckdb
