//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/statement/drop_sequence_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_data.hpp"
#include "parser/sql_statement.hpp"

namespace duckdb {

class DropSequenceStatement : public SQLStatement {
public:
	DropSequenceStatement() : SQLStatement(StatementType::DROP_SEQUENCE), info(make_unique<DropSequenceInformation>()){};

	string ToString() const override {
		return "DROP SEQUENCE";
	}

	bool Equals(const SQLStatement *other_) const override {
		if (!SQLStatement::Equals(other_)) {
			return false;
		}
		throw NotImplementedException("Equality not implemented!");
	}
	unique_ptr<DropSequenceInformation> info;
};

} // namespace duckdb
