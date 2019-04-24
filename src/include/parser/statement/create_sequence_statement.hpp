//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/statement/create_sequence_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_data.hpp"
#include "parser/sql_statement.hpp"

namespace duckdb {

class CreateSequenceStatement : public SQLStatement {
public:
	CreateSequenceStatement()
	    : SQLStatement(StatementType::CREATE_SEQUENCE), info(make_unique<CreateSequenceInformation>()){};

	string ToString() const override {
		return "CREATE SEQUENCE";
	}

	bool Equals(const SQLStatement *other_) const override {
		if (!SQLStatement::Equals(other_)) {
			return false;
		}
		throw NotImplementedException("Equality not implemented!");
	}

	unique_ptr<CreateSequenceInformation> info;
};

} // namespace duckdb
