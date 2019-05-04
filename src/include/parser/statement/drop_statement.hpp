//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/statement/drop_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_data.hpp"
#include "parser/sql_statement.hpp"

namespace duckdb {

class DropStatement : public SQLStatement {
public:
	DropStatement() : SQLStatement(StatementType::DROP), info(make_unique<DropInformation>()){};

	string ToString() const override {
		return "DROP";
	}

	bool Equals(const SQLStatement *other_) const override {
		if (!SQLStatement::Equals(other_)) {
			return false;
		}
		throw NotImplementedException("Equality not implemented!");
	}
	unique_ptr<DropInformation> info;
};

} // namespace duckdb
