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

	unique_ptr<CreateSequenceInformation> info;
};

} // namespace duckdb
