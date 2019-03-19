//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/statement/deallocate_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_data.hpp"
#include "parser/sql_statement.hpp"

namespace duckdb {

class DeallocateStatement : public SQLStatement {
public:
	DeallocateStatement(string name) : SQLStatement(StatementType::DEALLOCATE), name(name){};
	string ToString() const override {
		return "Deallocate";
	}

	bool Equals(const SQLStatement *other_) const override {
		if (!SQLStatement::Equals(other_)) {
			return false;
		}
		throw NotImplementedException("Equality not implemented!");
	}

	string name;

	// TODO
};
} // namespace duckdb
